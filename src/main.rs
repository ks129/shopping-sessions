//! Shopping Sessions pipeline. See rules.md.
//!
//! Every stage is a linear pass, parallel over T worker threads, with two
//! hash-partitioned shuffles in between:
//!
//!   1. parse events (mmap, chunked)      -> partitions by hash(event_id)
//!      (meanwhile: parse links, union-find, name each person)
//!   2. dedup each event_id partition     -> partitions by hash(person name)
//!   3. parse clicks                      -> partitions by hash(person name)
//!   4. each person partition: group by person, sort by ts, sessionize,
//!      credit/assist, aggregate -> user_daily part file, campaign counters
//!   5. merge campaign counters -> campaign_daily

mod output;
mod parse;

use std::fs::{self, File};
use std::mem::size_of;
use std::hash::{BuildHasherDefault, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::thread;
use std::time::Instant;

use output::Col;
use parse::{for_each_line, parse_ingest, parse_price, parse_ts, DAY};

const GAP: u32 = 30 * 60 * 1000;
const WEEK: i64 = 7 * DAY;
const PB: u32 = 9;
const P: usize = 1 << PB;

// ---------------------------------------------------------------------------
// A packed reference to bytes inside a memory-mapped input file:
// 48-bit pointer + 16-bit length (0xFFFF = pointer to a leaked slice).

#[derive(Clone, Copy)]
#[repr(transparent)]
struct S(u64);
const LONG: u64 = 0xFFFF;
impl S {
    #[inline(always)]
    fn new(b: &'static [u8]) -> S {
        let len = b.len() as u64;
        if len < LONG {
            S(b.as_ptr() as u64 | (len << 48))
        } else {
            let bx: &'static &'static [u8] = Box::leak(Box::new(b));
            S(bx as *const &'static [u8] as u64 | (LONG << 48))
        }
    }
    #[inline(always)]
    fn get(self) -> &'static [u8] {
        let len = self.0 >> 48;
        let p = (self.0 & ((1u64 << 48) - 1)) as *const u8;
        unsafe {
            if len != LONG {
                std::slice::from_raw_parts(p, len as usize)
            } else {
                *(p as *const &'static [u8])
            }
        }
    }
}

#[derive(Default)]
struct IdHasher(u64);
impl Hasher for IdHasher {
    fn write(&mut self, _: &[u8]) {
        unreachable!()
    }
    #[inline(always)]
    fn write_u64(&mut self, x: u64) {
        self.0 = self.0.rotate_left(32) ^ x
    }
    #[inline(always)]
    fn write_u128(&mut self, x: u128) {
        self.0 = x as u64
    }
    #[inline(always)]
    fn finish(&self) -> u64 {
        // Keys are already random hashes, but partitions fix their top bits
        // and some halves are only 32 bits wide: one multiply spreads them.
        self.0.wrapping_mul(0x9E37_79B9_7F4A_7C15)
    }
}
type HMap128 = hashbrown::HashMap<(u64, u64), u32, BuildHasherDefault<IdHasher>>;

#[inline(always)]
fn part_of(h: u64) -> usize {
    (h >> (64 - PB)) as usize
}

// ---------------------------------------------------------------------------
// Input files

fn list_dir(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let Ok(rd) = fs::read_dir(dir) else { return out };
    let mut paths: Vec<PathBuf> = rd.filter_map(|e| e.ok().map(|e| e.path())).collect();
    paths.sort();
    for p in paths {
        let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if !name.ends_with(".csv") || name.starts_with('.') || !p.is_file() {
            continue;
        }
        out.push(p);
    }
    out
}

fn map_file(p: &Path) -> &'static [u8] {
    let f = File::open(p).unwrap_or_else(|e| panic!("open {}: {e}", p.display()));
    if f.metadata().map(|m| m.len()).unwrap_or(0) == 0 {
        return &[];
    }
    // No MAP_POPULATE: faulting pages in from the parsing threads is cheaper
    // than one thread mapping all of them up front.
    let m = unsafe { memmap2::MmapOptions::new().map(&f) }.expect("mmap");
    let m: &'static memmap2::Mmap = Box::leak(Box::new(m));
    &m[..]
}

/// Everything after the header line.
fn body(data: &'static [u8]) -> &'static [u8] {
    match memchr::memchr(b'\n', data) {
        Some(i) => &data[i + 1..],
        None => &[],
    }
}

/// Splits the bodies of `files` into chunks of whole lines of about `target` bytes.
fn chunks(files: &[&'static [u8]], target: usize) -> Vec<&'static [u8]> {
    let mut out = Vec::new();
    for &f in files {
        let b = body(f);
        let mut cur = 0;
        while cur < b.len() {
            let nominal = cur + target;
            if nominal >= b.len() {
                out.push(&b[cur..]);
                break;
            }
            let end = match memchr::memchr(b'\n', &b[nominal..]) {
                Some(i) => nominal + i + 1,
                None => b.len(),
            };
            out.push(&b[cur..end]);
            cur = end;
        }
    }
    // Largest first for better load balance.
    out.sort_by_key(|c| std::cmp::Reverse(c.len()));
    out
}

fn manifest_ts(text: &str, key: &str) -> i64 {
    let at = text.find(&format!("\"{key}\"")).expect("manifest key") + key.len() + 2;
    let rest = &text[at..];
    let a = rest.find('"').unwrap() + 1;
    let b = a + rest[a..].find('"').unwrap();
    parse_ts(rest[a..b].as_bytes()).expect("manifest timestamp")
}

// ---------------------------------------------------------------------------
// Linked IDs

/// A linked ID, keyed by the 128-bit hash of the ID (collisions at 128 bits
/// are not a practical concern): its person's name and name hash.
#[derive(Clone, Copy, Default)]
#[repr(C)]
struct LEnt {
    lo: u64,
    hi: u64,
    name: S,
    nlo: u64,
}
impl Default for S {
    fn default() -> S {
        S(0)
    }
}

/// Linked-ID lookup table: buckets of 16 slots, picked by the top bits of
/// the ID hash. Each bucket's 32-bit tags fill one cache line (0 = empty),
/// apart from the 32-byte entries, so an ID that is not linked costs one
/// line, a linked one two. The top LB bits pick a region of buckets, so the
/// table can be built in parallel; a full bucket spills to the next one in
/// its region.
struct Links {
    bbits: u32,
    rb: usize,
    tags: &'static mut [u32],
    ents: &'static mut [LEnt],
    n: usize,
}

const MISS: u32 = u32::MAX;
const SLOW: u32 = u32::MAX - 1;

/// A zero-filled slice in huge pages (where the kernel allows), never freed.
fn huge_zeroed<T: 'static>(n: usize) -> &'static mut [T] {
    const HUGE: usize = 2 << 20;
    let bytes = (n * std::mem::size_of::<T>()).max(1) + HUGE;
    unsafe {
        let p = libc::mmap(
            std::ptr::null_mut(),
            bytes,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
            -1,
            0,
        );
        assert!(p != libc::MAP_FAILED, "mmap failed");
        let aligned = ((p as usize + HUGE - 1) & !(HUGE - 1)) as *mut libc::c_void;
        #[cfg(target_os = "linux")]
        libc::madvise(aligned, bytes - HUGE, libc::MADV_HUGEPAGE);
        std::slice::from_raw_parts_mut(aligned as *mut T, n)
    }
}

#[inline(always)]
fn prefetch<T>(p: *const T) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        core::arch::x86_64::_mm_prefetch(p as *const i8, core::arch::x86_64::_MM_HINT_T0)
    }
    #[cfg(target_arch = "aarch64")]
    unsafe {
        core::arch::asm!("prfm pldl1keep, [{0}]", in(reg) p, options(nostack, readonly, preserves_flags))
    }
}

const LB: u32 = 8;
const LP: usize = 1 << LB;

impl Links {
    #[inline(always)]
    fn tag(lo: u64) -> u32 {
        lo as u32 | 1
    }
    #[inline(always)]
    fn bucket(&self, lo: u64) -> usize {
        (lo >> (64 - self.bbits)) as usize
    }
    #[inline(always)]
    fn prefetch_tags(&self, lo: u64) {
        if self.n != 0 {
            prefetch(unsafe { self.tags.as_ptr().add(self.bucket(lo) * 16) });
        }
    }
    /// Slot of the first tag match in the home bucket, MISS if the bucket
    /// has no match and an empty slot, SLOW if it has no match and is full.
    #[inline(always)]
    fn find_slot(&self, lo: u64) -> u32 {
        if self.n == 0 {
            return MISS;
        }
        let b = self.bucket(lo) * 16;
        let t = Self::tag(lo);
        let (hit, empty) = tag_masks(unsafe { self.tags.as_ptr().add(b) }, t);
        if hit != 0 {
            (b + hit.trailing_zeros() as usize) as u32
        } else if empty != 0 {
            MISS
        } else {
            SLOW
        }
    }
    /// The entry of a linked ID, given find_slot's answer for it.
    #[inline(always)]
    fn finish(&self, slot: u32, lo: u64, hi: u64) -> Option<&LEnt> {
        if slot == MISS {
            return None;
        }
        if slot != SLOW {
            let e = unsafe { self.ents.get_unchecked(slot as usize) };
            if e.lo == lo && e.hi == hi {
                return Some(e);
            }
        }
        self.find(lo, hi)
    }
    /// Full lookup: every tag match of every bucket from the home one on.
    #[cold]
    #[inline(never)]
    fn find(&self, lo: u64, hi: u64) -> Option<&LEnt> {
        if self.n == 0 {
            return None;
        }
        let t = Self::tag(lo);
        let home = self.bucket(lo);
        let base = home / self.rb * self.rb;
        let mut b = home;
        loop {
            let mut empty = false;
            for i in b * 16..b * 16 + 16 {
                if self.tags[i] == t && self.ents[i].lo == lo && self.ents[i].hi == hi {
                    return Some(&self.ents[i]);
                }
                empty |= self.tags[i] == 0;
            }
            if empty {
                return None;
            }
            b += 1;
            if b == base + self.rb {
                b = base;
            }
        }
    }
}

/// Bit masks of the 16 tags at `p` equal to `t` and equal to 0.
#[inline(always)]
fn tag_masks(p: *const u32, t: u32) -> (u32, u32) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    unsafe {
        use core::arch::x86_64::*;
        let a = _mm256_loadu_si256(p as *const __m256i);
        let b = _mm256_loadu_si256(p.add(8) as *const __m256i);
        let tv = _mm256_set1_epi32(t as i32);
        let z = _mm256_setzero_si256();
        let m = |x: __m256i| _mm256_movemask_ps(_mm256_castsi256_ps(x)) as u32;
        let hit = m(_mm256_cmpeq_epi32(a, tv)) | m(_mm256_cmpeq_epi32(b, tv)) << 8;
        let empty = m(_mm256_cmpeq_epi32(a, z)) | m(_mm256_cmpeq_epi32(b, z)) << 8;
        (hit, empty)
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
    {
        let tags = unsafe { std::slice::from_raw_parts(p, 16) };
        let (mut hit, mut empty) = (0u32, 0u32);
        for (i, &x) in tags.iter().enumerate() {
            hit |= ((x == t) as u32) << i;
            empty |= ((x == 0) as u32) << i;
        }
        (hit, empty)
    }
}

/// Resolves the person of each item, in a three-stage prefetch pipeline:
/// prefetch the item's tag line; D items later compare tags and prefetch the
/// entry; D items after that call `f(item, entry)` (None = not linked).
#[inline(always)]
fn resolve_all<T>(links: &Links, items: &[T], key: impl Fn(&T) -> (u64, u64), slots: &mut Vec<u32>, mut f: impl FnMut(&T, Option<&LEnt>)) {
    const D: usize = 16;
    let n = items.len();
    slots.clear();
    slots.resize(n, MISS);
    for k in 0..n + 2 * D {
        if k < n {
            links.prefetch_tags(key(&items[k]).0);
        }
        if k >= D && k - D < n {
            let j = k - D;
            let s = links.find_slot(key(&items[j]).0);
            slots[j] = s;
            if s < SLOW {
                prefetch(unsafe { links.ents.as_ptr().add(s as usize) });
            }
        }
        if k >= 2 * D {
            let j = k - 2 * D;
            let (lo, hi) = key(&items[j]);
            f(&items[j], links.finish(slots[j], lo, hi));
        }
    }
}

/// A small exact lookup table from byte strings to u32, keyed by the short
/// hash (open addressing; the bytes are compared on a hash match).
struct BTab {
    keys: Vec<(u64, u32)>,
    strs: Vec<&'static [u8]>,
    mask: usize,
}

impl BTab {
    /// A table for up to `cap` distinct keys.
    fn new(cap: usize) -> Self {
        let size = (cap * 4).next_power_of_two().max(16);
        BTab { keys: vec![(0, u32::MAX); size], strs: Vec::new(), mask: size - 1 }
    }
    /// Adds `k` unless present; true if it was new (its index is the count
    /// of distinct keys before it).
    fn insert(&mut self, k: &'static [u8]) -> bool {
        if self.get(k).is_some() {
            return false;
        }
        let h = h128(k).0;
        let mut s = h as usize & self.mask;
        while self.keys[s].1 != u32::MAX {
            s = (s + 1) & self.mask;
        }
        self.keys[s] = (h, self.strs.len() as u32);
        self.strs.push(k);
        true
    }
    /// Index (insertion order of distinct keys) of `k`, if present.
    #[inline(always)]
    fn get(&self, k: &[u8]) -> Option<u32> {
        let h = h128(k).0;
        let mut s = h as usize & self.mask;
        loop {
            let (kh, i) = unsafe { *self.keys.get_unchecked(s) };
            if i == u32::MAX {
                return None;
            }
            if kh == h && self.strs[i as usize] == k {
                return Some(i);
            }
            s = (s + 1) & self.mask;
        }
    }
}

/// 128-bit hash of an ID. Up to 16 bytes: two independent folded 64x64-bit
/// multiplies over two (overlapping) 8-byte loads and the length; longer: xxh3.
#[inline(always)]
fn h128(b: &[u8]) -> (u64, u64) {
    let n = b.len();
    if n > 16 {
        return h128_long(b);
    }
    let (a, c) = if n >= 8 {
        (u64::from_le_bytes(b[..8].try_into().unwrap()), u64::from_le_bytes(b[n - 8..].try_into().unwrap()))
    } else if n >= 4 {
        (u32::from_le_bytes(b[..4].try_into().unwrap()) as u64, u32::from_le_bytes(b[n - 4..].try_into().unwrap()) as u64)
    } else if n > 0 {
        (b[0] as u64 | (b[n / 2] as u64) << 8, b[n - 1] as u64)
    } else {
        (0, 0)
    };
    #[inline(always)]
    fn fold(x: u64, y: u64) -> u64 {
        let m = x as u128 * y as u128;
        (m as u64) ^ ((m >> 64) as u64)
    }
    const K: [u64; 4] = [0xA076_1D64_78BD_642F, 0xE703_7ED1_A0B4_28DB, 0x8EBC_6AF0_9C88_C6E3, 0x5899_65CC_7537_4CC3];
    let l = n as u64;
    (fold(a ^ K[0], c ^ K[1] ^ l.rotate_left(32)), fold(a ^ K[2] ^ l, c ^ K[3]))
}

#[cold]
#[inline(never)]
fn h128_long(b: &[u8]) -> (u64, u64) {
    let h = xxhash_rust::xxh3::xxh3_128(b);
    (h as u64, (h >> 64) as u64)
}

/// Big-endian first 8 bytes, zero padded: orders like the bytes unless equal.
#[inline(always)]
fn key8(b: &[u8]) -> u64 {
    let mut k = [0u8; 8];
    let n = b.len().min(8);
    k[..n].copy_from_slice(&b[..n]);
    u64::from_be_bytes(k)
}

#[derive(Clone, Copy)]
struct SendPtr<T>(*mut T);
unsafe impl<T> Send for SendPtr<T> {}
unsafe impl<T> Sync for SendPtr<T> {}
impl<T> SendPtr<T> {
    #[inline(always)]
    fn at(&self, i: usize) -> *mut T {
        unsafe { self.0.add(i) }
    }
}

/// Like par_items, but returns each item's result in item order.
fn par_map<R: Send, St: Send, F>(t: usize, n: usize, init: impl Fn() -> St + Sync, f: F) -> Vec<R>
where
    F: Fn(&mut St, usize) -> R + Sync,
{
    let mut out: Vec<Option<R>> = (0..n).map(|_| None).collect();
    let ptr = SendPtr(out.as_mut_ptr());
    par_items(t, n, init, |st, i| {
        let r = f(st, i);
        unsafe { *ptr.at(i) = Some(r) };
    });
    out.into_iter().map(|r| r.unwrap()).collect()
}

/// Builds the person of every linked ID: parallel parse and hash, parallel
/// interning by hash region, union-find, smallest ID per person, and the
/// region-partitioned lookup table.
fn build_links(files: &[&'static [u8]], nt: usize) -> Links {
    #[derive(Clone, Copy)]
    struct Ep {
        lo: u64,
        hi: u64,
        s: S,
        back: u32,
    }
    struct A {
        parts: Vec<Vec<Ep>>,
        npairs: u32,
    }
    let ch = chunks(files, 1 << 20);
    let total: usize = files.iter().map(|f| f.len()).sum();
    let per = total / 20 * 2 / nt / LP * 5 / 4 + 16; // endpoints per thread and region, with slack
    let a: Vec<A> = par_items(
        nt,
        ch.len(),
        || A { parts: (0..LP).map(|_| Vec::with_capacity(per)).collect(), npairs: 0 },
        |st, i| {
            for_each_line::<2, _>(ch[i], |f| {
                if f[0].is_empty() || f[1].is_empty() {
                    return;
                }
                let k = st.npairs;
                st.npairs += 1;
                for side in 0..2 {
                    let (lo, hi) = h128(f[side]);
                    st.parts[(lo >> (64 - LB)) as usize].push(Ep { lo, hi, s: S::new(f[side]), back: k * 2 + side as u32 });
                }
            });
        },
    );
    // Per thread, per endpoint: (region << 24 | local id).
    let mut ep_ids: Vec<Vec<u32>> = a.iter().map(|w| vec![0u32; w.npairs as usize * 2]).collect();
    let ep_ptrs: Vec<SendPtr<u32>> = ep_ids.iter_mut().map(|v| SendPtr(v.as_mut_ptr())).collect();
    // Per region: its IDs as (bytes, lo, hi, key8).
    let regions: Vec<Vec<(S, u64, u64, u64)>> = par_map(nt, LP, HMap128::default, |map, p| {
        map.clear();
        let mut ids = Vec::new();
        for (t, w) in a.iter().enumerate() {
            for e in &w.parts[p] {
                let n = ids.len() as u32;
                let id = *map.entry((e.lo, e.hi)).or_insert(n);
                if id == n {
                    ids.push((e.s, e.lo, e.hi, key8(e.s.get())));
                }
                assert!(id < 1 << 24);
                unsafe { *ep_ptrs[t].at(e.back as usize) = ((p as u32) << 24) | id };
            }
        }
        ids
    });
    drop(a);
    let mut off = vec![0u32; LP + 1];
    for p in 0..LP {
        off[p + 1] = off[p] + regions[p].len() as u32;
    }
    let n = off[LP] as usize;
    if n == 0 {
        return Links { bbits: LB, rb: 1, tags: &mut [], ents: &mut [], n: 0 };
    }
    let gid = |packed: u32| off[(packed >> 24) as usize] + (packed & 0xFF_FFFF);
    // Lock-free parallel union-find: a root only ever gets a smaller parent
    // (by compare-and-swap) and path halving only shortcuts to smaller ids, so
    // parent[x] <= x always holds and one ascending pass flattens it.
    let par: Vec<AtomicU32> = (0..n as u32).map(AtomicU32::new).collect();
    let afind = |mut x: u32| -> u32 {
        loop {
            let p = par[x as usize].load(Ordering::Relaxed);
            if p == x {
                return x;
            }
            let gp = par[p as usize].load(Ordering::Relaxed);
            if gp != p {
                let _ = par[x as usize].compare_exchange_weak(p, gp, Ordering::Relaxed, Ordering::Relaxed);
            }
            x = gp;
        }
    };
    const UST: usize = 1 << 14;
    let jobs: Vec<(usize, usize)> = ep_ids.iter().enumerate().flat_map(|(t, w)| (0..w.len().div_ceil(UST)).map(move |c| (t, c))).collect();
    par_items(nt, jobs.len(), || (), |_, j| {
        let (t, c) = jobs[j];
        let w = &ep_ids[t];
        for pair in w[c * UST..((c + 1) * UST).min(w.len())].chunks_exact(2) {
            let (mut a, mut b) = (gid(pair[0]), gid(pair[1]));
            loop {
                a = afind(a);
                b = afind(b);
                if a == b {
                    break;
                }
                let (hi, lo) = if a > b { (a, b) } else { (b, a) };
                match par[hi as usize].compare_exchange(hi, lo, Ordering::Relaxed, Ordering::Relaxed) {
                    Ok(_) => break,
                    Err(_) => {
                        a = hi;
                        b = lo;
                    }
                }
            }
        }
    });
    let mut parent: Vec<u32> = par.into_iter().map(|a| a.into_inner()).collect();
    for i in 0..n {
        parent[i] = parent[parent[i] as usize];
    }
    // Smallest ID per person: flatten the regions in parallel, then a
    // lock-free minimum per root (key8 first, full bytes on a tie).
    let flat: &'static mut [(S, u64, u64, u64)] = huge_zeroed(n);
    {
        let fp = SendPtr(flat.as_mut_ptr());
        par_items(nt, LP, || (), |_, p| {
            let dst = unsafe { std::slice::from_raw_parts_mut(fp.at(off[p] as usize), regions[p].len()) };
            dst.copy_from_slice(&regions[p]);
        });
    }
    let flat = &*flat;
    let best: Vec<AtomicU32> = (0..n).map(|_| AtomicU32::new(u32::MAX)).collect();
    let less = |i: usize, j: usize| {
        let (x, y) = (&flat[i], &flat[j]);
        x.3 < y.3 || (x.3 == y.3 && x.0.get() < y.0.get())
    };
    const STEP: usize = 1 << 14;
    par_items(nt, n.div_ceil(STEP), || (), |_, c| {
        for i in c * STEP..((c + 1) * STEP).min(n) {
            let b = &best[parent[i] as usize];
            let mut cur = b.load(Ordering::Relaxed);
            while cur == u32::MAX || less(i, cur as usize) {
                match b.compare_exchange_weak(cur, i as u32, Ordering::Relaxed, Ordering::Relaxed) {
                    Ok(_) => break,
                    Err(v) => cur = v,
                }
            }
        }
    });
    let best: Vec<u32> = best.into_iter().map(|a| a.into_inner()).collect();
    // Lookup table: rb buckets per region, a load of at most about 0.6.
    let max = regions.iter().map(|r| r.len()).max().unwrap();
    let rb = (max * 10 / 6 / 16 + 1).next_power_of_two();
    let bbits = LB + rb.trailing_zeros();
    let tags: &'static mut [u32] = huge_zeroed(LP * rb * 16);
    let ents: &'static mut [LEnt] = huge_zeroed(LP * rb * 16);
    let (tp, ep) = (SendPtr(tags.as_mut_ptr()), SendPtr(ents.as_mut_ptr()));
    par_items(nt, LP, || (), |_, p| {
        let base = p * rb;
        for (local, &(_, lo, hi, _)) in regions[p].iter().enumerate() {
            let g = off[p] as usize + local;
            let name = flat[best[parent[g] as usize] as usize];
            let mut b = (lo >> (64 - bbits)) as usize;
            debug_assert_eq!(b / rb, p);
            unsafe {
                'place: loop {
                    for i in b * 16..b * 16 + 16 {
                        if *tp.at(i) == 0 {
                            *tp.at(i) = Links::tag(lo);
                            *ep.at(i) = LEnt { lo, hi, name: name.0, nlo: name.1 };
                            break 'place;
                        }
                    }
                    b += 1;
                    if b == base + rb {
                        b = base;
                    }
                }
            }
        }
    });
    Links { bbits, rb, tags, ents, n }
}

// ---------------------------------------------------------------------------
// Records

/// Dedup record of a parsed event line, in its event_id partition: the low
/// 64 bits of the event_id's 128-bit hash (the partition comes from the high
/// bits), its ingest time, and the arena index of its E2 (or NONE).
#[derive(Clone, Copy)]
#[repr(C)]
struct D {
    h: u64,
    loc: u32,
    /// ingest_ts in seconds since `base`, clamped
    ingest: i32,
}
const NONE: u32 = u32::MAX;

/// A surviving event, in its person partition. The person is the low 55
/// bits of nh (the partition is its top 9 bits, so they are free and hold
/// the category's high byte); pc = price (24 bits, 0 = not a purchase) |
/// category low byte.
#[derive(Clone, Copy)]
#[repr(C)]
struct E2 {
    nh: u64,
    name: S,
    ts: u32,
    pc: u32,
}
const LOW55: u64 = (1 << 55) - 1;

/// A valid click, in its person partition (person = low 55 bits of nh).
#[derive(Clone, Copy)]
#[repr(C)]
struct C2 {
    nh: u64,
    cid: S,
    ts: u32,
    camp: u32,
}

#[derive(Clone, Copy)]
struct Ev {
    ts: u32,
    price: u32,
    cat: u16,
}

#[derive(Clone, Copy)]
struct Cl {
    ts: u32,
    camp: u32,
    cid: S,
}

struct Sess {
    start: u32,
    rev: i64,
    purch: bool,
    day: u16,
}

struct URow {
    name: S,
    day: i32,
    sessions: u32,
    events: u32,
    rev: i64,
    top: i32,
}

/// A big anonymous mapping (huge pages where the kernel allows, pages
/// faulted only when touched) that threads carve fixed-size blocks from.
struct Arena<T> {
    base: *mut T,
    cap: usize,
    next: AtomicUsize,
}
unsafe impl<T> Sync for Arena<T> {}
unsafe impl<T> Send for Arena<T> {}

const BLK: usize = 256;

impl<T: Copy + 'static> Arena<T> {
    fn new(cap: usize) -> Self {
        let cap = cap.max(BLK) + BLK;
        Arena { base: huge_zeroed::<T>(cap).as_mut_ptr(), cap, next: AtomicUsize::new(0) }
    }
    fn used(&self) -> usize {
        self.next.load(Ordering::Relaxed)
    }
    #[inline(always)]
    fn slice(&self, s: u32, l: u32) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.base.add(s as usize), l as usize) }
    }
}

/// One thread's writer into an arena, with `np` partitions: the open block
/// of each partition and the finished blocks (start, len) per partition.
struct PW {
    cur: Vec<(u32, u32)>,
    lists: Vec<Vec<(u32, u32)>>,
    /// Write-combining: each partition's current group of G records is built
    /// here (cache-resident) and copied out as whole cache lines with
    /// non-temporal stores, so arena lines are never read before written.
    stage: Vec<Line>,
    /// bytes per group (lcm of the record size and 64), records per group
    gbytes: usize,
    g: u32,
}

#[derive(Clone, Copy)]
#[repr(C, align(64))]
struct Line([u8; 64]);

/// Bytes in a write-combining group of records of `size` bytes: the least
/// multiple of 64 that is a multiple of `size`.
const fn group_bytes(size: usize) -> usize {
    let mut g = 64;
    while g % size != 0 {
        g += 64;
    }
    g
}

/// Copies whole 32-byte chunks with non-temporal stores (`dst`, `src` 32-aligned).
#[inline(always)]
unsafe fn nt_copy(dst: *mut u8, src: *const u8, bytes: usize) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx"))]
    {
        use core::arch::x86_64::*;
        let mut o = 0;
        while o < bytes {
            let v = _mm256_load_si256(src.add(o) as *const __m256i);
            _mm256_stream_si256(dst.add(o) as *mut __m256i, v);
            o += 32;
        }
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx")))]
    std::ptr::copy_nonoverlapping(src, dst, bytes);
}

impl PW {
    fn new(np: usize, size: usize) -> Self {
        let gbytes = group_bytes(size);
        let g = (gbytes / size) as u32;
        assert!(BLK % g as usize == 0);
        PW {
            cur: vec![(0, 0); np],
            lists: (0..np).map(|_| Vec::new()).collect(),
            stage: vec![Line([0; 64]); np * gbytes / 64],
            gbytes,
            g,
        }
    }
    /// Appends `v` to partition `p`; returns its arena index.
    #[inline(always)]
    fn push<T: Copy>(&mut self, a: &Arena<T>, p: usize, v: T) -> u32 {
        let c = unsafe { self.cur.get_unchecked_mut(p) };
        if c.0 == c.1 {
            self.refill(a, p);
        }
        let c = unsafe { self.cur.get_unchecked_mut(p) };
        let i = c.0;
        c.0 += 1;
        // group size and bytes are constants per record type (no division)
        let (gb, g) = (group_bytes(size_of::<T>()), (group_bytes(size_of::<T>()) / size_of::<T>()) as u32);
        let k = i % g;
        unsafe {
            let st = (self.stage.as_mut_ptr() as *mut u8).add(p * gb);
            (st as *mut T).add(k as usize).write(v);
            if k == g - 1 {
                nt_copy(a.base.add((i - k) as usize) as *mut u8, st, gb);
            }
        }
        i
    }
    #[cold]
    #[inline(never)]
    fn refill<T: Copy>(&mut self, a: &Arena<T>, p: usize) {
        let c = self.cur[p];
        if c.1 != 0 {
            self.lists[p].push((c.1 - BLK as u32, BLK as u32));
        }
        let s = a.next.fetch_add(BLK, Ordering::Relaxed);
        assert!(s + BLK <= a.cap && s + BLK < u32::MAX as usize, "arena full");
        self.cur[p] = (s as u32, (s + BLK) as u32);
    }
    fn finish<T: Copy>(&mut self, a: &Arena<T>) {
        for p in 0..self.cur.len() {
            let c = self.cur[p];
            if c.1 != 0 {
                let start = c.1 - BLK as u32;
                if c.0 > start {
                    self.lists[p].push((start, c.0 - start));
                }
                let k = c.0 % self.g;
                if k > 0 {
                    // the partial last group, plainly
                    unsafe {
                        let st = (self.stage.as_ptr() as *const u8).add(p * self.gbytes);
                        std::ptr::copy_nonoverlapping(st as *const T, a.base.add((c.0 - k) as usize), k as usize);
                    }
                }
            }
            self.cur[p] = (0, 0);
        }
        sfence();
    }
}

/// Orders this thread's earlier non-temporal stores before later stores.
#[inline(always)]
fn sfence() {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        core::arch::x86_64::_mm_sfence();
    }
}

/// All the blocks of partition `p`, over the writers of every thread.
fn blocks<'a, T: Copy + 'static>(a: &'a Arena<T>, ws: &'a [PW], p: usize) -> impl Iterator<Item = (u32, &'a [T])> + 'a {
    ws.iter().flat_map(move |w| w.lists[p].iter().map(move |&(s, l)| (s, a.slice(s, l))))
}

/// Like par_items, then `then(state, worker)` in each worker once no items are left.
fn par_items_then<St: Send, F, G>(t: usize, n: usize, init: impl Fn() -> St + Sync, f: F, then: G)
where
    F: Fn(&mut St, usize) + Sync,
    G: Fn(&mut St, usize) + Sync,
{
    let next = AtomicUsize::new(0);
    thread::scope(|sc| {
        for w in 0..t {
            let (next, init, f, then) = (&next, &init, &f, &then);
            sc.spawn(move || {
                let mut st = init();
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    if i >= n {
                        break;
                    }
                    f(&mut st, i);
                }
                then(&mut st, w);
            });
        }
    });
}

/// Runs `f(worker, item)` for items 0..n on t threads, each with its own state.
fn par_items<St: Send, F>(t: usize, n: usize, init: impl Fn() -> St + Sync, f: F) -> Vec<St>
where
    F: Fn(&mut St, usize) + Sync,
{
    let next = AtomicUsize::new(0);
    thread::scope(|sc| {
        let hs: Vec<_> = (0..t)
            .map(|_| {
                sc.spawn(|| {
                    let mut st = init();
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= n {
                            break;
                        }
                        f(&mut st, i);
                    }
                    st
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

fn main() {
    let t0 = Instant::now();
    let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
    let args: Vec<String> = std::env::args().collect();
    let data = PathBuf::from(if args.len() >= 3 { args[1].clone() } else { env("DATA_DIR", "/data") });
    let out = PathBuf::from(if args.len() >= 3 { args[2].clone() } else { env("OUTPUT_DIR", "/output") });
    let nt: usize = std::env::var("NCPU")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| thread::available_parallelism().map_or(8, |n| n.get()))
        .max(1);
    let lap = |what: &str| eprintln!("[{:7.3}s] {what}", t0.elapsed().as_secs_f64());

    // ---- small inputs
    let manifest = fs::read_to_string(data.join("manifest.json")).expect("manifest.json");
    let ws = manifest_ts(&manifest, "window_start");
    let we = manifest_ts(&manifest, "window_end");
    let base = ws - WEEK; // all stored times are u32 ms since base
    assert!(we - base < u32::MAX as i64, "window too long");
    let d0 = ws.div_euclid(DAY);
    let nd = ((we - 1).div_euclid(DAY) - d0 + 1) as usize;
    let day_of = |rel: u32| ((base + rel as i64).div_euclid(DAY) - d0) as u16;

    let mut cat_names: Vec<&'static [u8]> = Vec::new();
    let mut cat_ids: hashbrown::HashMap<&'static [u8], u16> = hashbrown::HashMap::new();
    let products_csv = body(map_file(&data.join("products.csv")));
    let mut products = BTab::new(memchr::memchr_iter(b'\n', products_csv).count() + 1);
    let mut prod_cat: Vec<u16> = Vec::new(); // by product index; the first line of a product wins
    for_each_line::<2, _>(products_csv, |f| {
        let n = cat_names.len() as u16;
        let c = *cat_ids.entry(f[1]).or_insert(n);
        if c == n {
            cat_names.push(f[1]);
        }
        if products.insert(f[0]) {
            prod_cat.push(c);
        }
    });
    let unknown = cat_names.len() as u16;
    cat_names.push(b"unknown");
    let mut cat_rank = vec![0u32; cat_names.len()];
    {
        let mut order: Vec<usize> = (0..cat_names.len()).collect();
        order.sort_by(|&a, &b| cat_names[a].cmp(cat_names[b]));
        for (r, &c) in order.iter().enumerate() {
            cat_rank[c] = r as u32;
        }
    }

    let mut camp_ids: Vec<&'static [u8]> = Vec::new();
    let mut channels: Vec<&'static [u8]> = Vec::new();
    let campaigns_csv = body(map_file(&data.join("campaigns.csv")));
    let mut camps = BTab::new(memchr::memchr_iter(b'\n', campaigns_csv).count() + 1);
    for_each_line::<2, _>(campaigns_csv, |f| {
        if camps.insert(f[0]) {
            camp_ids.push(f[0]);
            channels.push(f[1]);
        }
    });
    let nc = camp_ids.len();
    let organic = nc as u32;

    let (ev_files, cl_files, ln_files) = {
        let dirs = [list_dir(&data.join("events")), list_dir(&data.join("clicks")), list_dir(&data.join("links"))];
        let all: Vec<&PathBuf> = dirs.iter().flatten().collect();
        let maps: Vec<&'static [u8]> = par_map(nt, all.len(), || (), |_, i| map_file(all[i]));
        let (a, b) = (dirs[0].len(), dirs[0].len() + dirs[1].len());
        (maps[..a].to_vec(), maps[a..b].to_vec(), maps[b..].to_vec())
    };
    let ev_bytes: usize = ev_files.iter().map(|f| f.len()).sum();
    let cl_bytes: usize = cl_files.iter().map(|f| f.len()).sum();
    lap(&format!("inputs mapped: {} event files ({} MB), {} click files, {} link files", ev_files.len(), ev_bytes >> 20, cl_files.len(), ln_files.len()));

    // ---- links first (parallel, fast), so events can be routed by person
    // Arenas for stage 1 (virtual; pages are faulted in as blocks are used).
    const DB: u32 = 11;
    const DP: usize = 1 << DB;
    let ea: &'static Arena<E2> = Box::leak(Box::new(Arena::new(ev_bytes / 12 + nt * P * BLK)));
    let da: &'static Arena<D> = Box::leak(Box::new(Arena::new(ev_bytes / 12 + nt * DP * BLK)));
    let ca: &'static Arena<C2> = Box::leak(Box::new(Arena::new(cl_bytes / 10 + nt * P * BLK)));
    let links = build_links(&ln_files, nt);
    lap(&format!("links: {} linked IDs", links.n));

    // ---- stage 1: parse events. Valid events go straight to their person
    // partition (E2); every parsed line also leaves a compact dedup record
    // (D) in its event_id partition pointing at its E2 (or NONE if invalid).
    let ingest_s = |ms: i64| (ms - base).div_euclid(1000).clamp(i32::MIN as i64, i32::MAX as i64) as i32;
    let ev_chunks = chunks(&ev_files, 1 << 20);
    let cl_chunks = chunks(&cl_files, 1 << 20);
    #[derive(Clone, Copy)]
    struct Pend {
        h: u64,
        user: S,
        ulo: u64,
        uhi: u64,
        ts: u32,
        pc: u32,
        cathi: u32,
        dp: u32,
        ingest: i32,
    }
    struct ParseSt {
        ev: PW,
        dd: PW,
        cl: PW,
        pend: Vec<Pend>,
        cpend: Vec<(S, u64, u64, S, u32, u32)>,
        slots: Vec<u32>,
    }
    let st1: Vec<ParseSt> = par_items(
        nt,
        ev_chunks.len() + cl_chunks.len(),
        || ParseSt { ev: PW::new(P, size_of::<E2>()), dd: PW::new(DP, size_of::<D>()), cl: PW::new(P, size_of::<C2>()), pend: Vec::new(), cpend: Vec::new(), slots: Vec::new() },
        |st, i| {
            if i >= ev_chunks.len() {
                // a click chunk
                st.cpend.clear();
                let cpend = &mut st.cpend;
                for_each_line::<4, _>(cl_chunks[i - ev_chunks.len()], |f| {
                    if f[0].is_empty() || f[1].is_empty() {
                        return;
                    }
                    let ts = match parse_ts(f[3]) {
                        Some(ts) if ts >= base && ts < we => ts,
                        _ => return,
                    };
                    let Some(camp) = camps.get(f[2]) else { return };
                    let (lo, hi) = h128(f[1]);
                    cpend.push((S::new(f[1]), lo, hi, S::new(f[0]), (ts - base) as u32, camp));
                });
                let cl = &mut st.cl;
                resolve_all(&links, &st.cpend, |c| (c.1, c.2), &mut st.slots, |c, e| {
                    let nh = e.map_or(c.1, |e| e.nlo);
                    cl.push(ca, part_of(nh), C2 { nh, cid: c.3, ts: c.4, camp: c.5 });
                });
                sfence();
                return;
            }
            st.pend.clear();
            let dd = &mut st.dd;
            let pend = &mut st.pend;
            for_each_line::<7, _>(ev_chunks[i], |f| {
                let eid = f[0];
                if eid.is_empty() {
                    return;
                }
                let Some(ingest) = parse_ingest(f[3]) else { return };
                let (h, hh) = h128(eid);
                let dp = (hh >> (64 - DB)) as u32;
                let mut valid = !f[1].is_empty() && !f[5].is_empty();
                let purchase = match f[4] {
                    b"view" | b"cart" => false,
                    b"purchase" => true,
                    _ => {
                        valid = false;
                        false
                    }
                };
                let mut ts_rel = 0u32;
                if valid {
                    match parse_ts(f[2]) {
                        Some(ts) if ts >= ws && ts < we && ingest - ts <= DAY => ts_rel = (ts - base) as u32,
                        _ => valid = false,
                    }
                }
                let (mut pc, mut cathi) = (0u32, 0u32);
                if valid && purchase {
                    match parse_price(f[6]) {
                        Some(p) => {
                            let cat = products.get(f[5]).map_or(unknown, |i| prod_cat[i as usize]) as u32;
                            pc = p | (cat & 0xFF) << 24;
                            cathi = cat >> 8;
                        }
                        None => valid = false,
                    }
                }
                if !valid {
                    dd.push(da, dp as usize, D { h, loc: NONE, ingest: ingest_s(ingest) });
                    return;
                }
                let (ulo, uhi) = h128(f[1]);
                pend.push(Pend { h, user: S::new(f[1]), ulo, uhi, ts: ts_rel, pc, cathi, dp, ingest: ingest_s(ingest) });
            });
            let (ev, dd) = (&mut st.ev, &mut st.dd);
            resolve_all(&links, &st.pend, |r| (r.ulo, r.uhi), &mut st.slots, |r, e| {
                let (name, nh) = e.map_or((r.user, r.ulo), |e| (e.name, e.nlo));
                let loc = ev.push(ea, part_of(nh), E2 { nh: (nh & LOW55) | (r.cathi as u64) << 55, name, ts: r.ts, pc: r.pc });
                dd.push(da, r.dp as usize, D { h: r.h, loc, ingest: r.ingest });
            });
            sfence();
        },
    );
    let (mut e2w, mut ddw, mut c2w) = (Vec::new(), Vec::new(), Vec::new());
    for mut s in st1 {
        s.ev.finish(ea);
        s.dd.finish(da);
        s.cl.finish(ca);
        e2w.push(s.ev);
        ddw.push(s.dd);
        c2w.push(s.cl);
    }
    lap("stage 1: events and clicks parsed");

    // ---- stage 2: dedup by event_id; mark losing copies in a bitmap over
    // the E2 arena
    assert!(ea.cap < (u32::MAX - 2) as usize);
    let losers: Vec<AtomicU64> = (0..ea.used() / 64 + 1).map(|_| AtomicU64::new(0)).collect();
    let lose = |loc: u32| {
        if loc != NONE {
            losers[(loc >> 6) as usize].fetch_or(1 << (loc & 63), Ordering::Relaxed);
        }
    };
    const EMPTY: u32 = u32::MAX - 1;
    let n1: usize = ddw.iter().map(|w| w.lists.iter().flatten().map(|b| b.1 as usize).sum::<usize>()).sum();
    par_items(nt, DP, Vec::<D>::new, |tab, p| {
        let n: usize = ddw.iter().map(|w| w.lists[p].iter().map(|b| b.1 as usize).sum::<usize>()).sum();
        if n == 0 {
            return;
        }
        let size = (n * 2).next_power_of_two();
        let mask = size - 1;
        tab.clear();
        tab.resize(size, D { h: 0, loc: EMPTY, ingest: 0 });
        for (_, blk) in blocks(da, &ddw, p) {
            for r in blk {
                let mut s = (r.h as usize) & mask;
                loop {
                    let c = unsafe { tab.get_unchecked_mut(s) };
                    if c.loc == EMPTY {
                        *c = *r;
                        break;
                    }
                    if c.h == r.h {
                        // Smallest ingest_ts wins; equal ones are identical copies.
                        if r.ingest < c.ingest {
                            lose(c.loc);
                            *c = *r;
                        } else {
                            lose(r.loc);
                        }
                        break;
                    }
                    s = (s + 1) & mask;
                }
            }
        }
    });
    lap(&format!("stage 2: {} event lines deduped", n1));

    // ---- stage 4: per person partition
    let mut order: Vec<usize> = (0..P).collect();
    let psize = |p: usize| {
        e2w.iter().chain(c2w.iter()).map(|w| w.lists[p].iter().map(|b| b.1 as usize).sum::<usize>()).sum::<usize>()
    };
    let sizes: Vec<usize> = (0..P).map(psize).collect();
    order.sort_by_key(|&p| std::cmp::Reverse(sizes[p]));

    /// Event of a partition while it is sorted: by ts (LSD radix), then
    /// stably by person, so each person's events end up in ts order.
    #[derive(Clone, Copy, Default)]
    struct R {
        ts: u32,
        pid: u32,
        pc: u32,
        cathi: u32,
    }
    #[derive(Clone, Copy)]
    struct CR {
        ts: u32,
        pid: u32,
        camp: u32,
        cid: S,
    }
    /// Person table of a partition: open addressing on nh, growing.
    struct PMap {
        keys: Vec<u64>,
        vals: Vec<u32>,
        bits: u32,
    }
    impl PMap {
        fn reset(&mut self, bits: u32) {
            self.bits = bits;
            self.keys.clear();
            self.keys.resize(1 << bits, 0);
            self.vals.clear();
            self.vals.resize(1 << bits, u32::MAX);
        }
        #[inline(always)]
        fn slot(&self, k: u64) -> usize {
            (k.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> (64 - self.bits)) as usize
        }
        #[inline(always)]
        fn get(&self, k: u64) -> u32 {
            let m = (1 << self.bits) - 1;
            let mut s = self.slot(k);
            loop {
                let v = unsafe { *self.vals.get_unchecked(s) };
                if v == u32::MAX || unsafe { *self.keys.get_unchecked(s) } == k {
                    return v;
                }
                s = (s + 1) & m;
            }
        }
        /// Id of k, inserting `n` if absent (returns n then).
        #[inline(always)]
        fn get_or(&mut self, k: u64, n: u32) -> u32 {
            let m = (1 << self.bits) - 1;
            let mut s = self.slot(k);
            loop {
                let v = unsafe { *self.vals.get_unchecked(s) };
                if v == u32::MAX {
                    self.keys[s] = k;
                    self.vals[s] = n;
                    return n;
                }
                if unsafe { *self.keys.get_unchecked(s) } == k {
                    return v;
                }
                s = (s + 1) & m;
            }
        }
        fn grow(&mut self) {
            let old: Vec<(u64, u32)> = self.keys.iter().zip(&self.vals).filter(|x| *x.1 != u32::MAX).map(|(&k, &v)| (k, v)).collect();
            let bits = self.bits + 1;
            self.reset(bits);
            for (k, v) in old {
                self.get_or(k, v);
            }
        }
    }
    /// LSD radix sort of `a` by a 32-bit key (3 passes of 11 bits, skipping
    /// passes whose digit is constant); the result is in `a`.
    fn radix<T: Copy>(a: &mut Vec<T>, b: &mut Vec<T>, key: impl Fn(&T) -> u32) {
        let n = a.len();
        if n <= 1 {
            return;
        }
        let mut hist = [[0u32; 2048]; 3];
        for x in a.iter() {
            let k = key(x);
            hist[0][(k & 2047) as usize] += 1;
            hist[1][((k >> 11) & 2047) as usize] += 1;
            hist[2][(k >> 22) as usize] += 1;
        }
        b.clear();
        b.reserve(n);
        unsafe { b.set_len(n) };
        for (d, h) in hist.iter_mut().enumerate() {
            let k0 = key(&a[0]);
            let dig = ((k0 >> (11 * d)) & 2047) as usize;
            if h[dig] as usize == n {
                continue; // every key has this digit
            }
            let mut sum = 0u32;
            for c in h.iter_mut() {
                let x = *c;
                *c = sum;
                sum += x;
            }
            for x in a.iter() {
                let dg = ((key(x) >> (11 * d)) & 2047) as usize;
                let i = h[dg];
                h[dg] = i + 1;
                unsafe { *b.get_unchecked_mut(i as usize) = *x };
            }
            std::mem::swap(a, b);
        }
    }
    struct PSt {
        map: PMap,
        names: Vec<S>,
        ra: Vec<R>,
        rb: Vec<R>,
        ca_: Vec<CR>,
        cb_: Vec<CR>,
        cnt: Vec<u32>,
        evs: Vec<Ev>,
        ccnt: Vec<u32>,
        cls: Vec<Cl>,
        sess: Vec<Sess>,
        day_last: Vec<u32>,
        catsum: Vec<i64>,
        touched: Vec<u16>,
        open_lo: Vec<u32>,
        open_hi: Vec<u32>,
        open_t: Vec<u32>,
        camp: Vec<[i64; 4]>,
        rows: Vec<URow>,
    }
    let ncat = cat_names.len();
    let ud = out.join("user_daily");
    let cd = out.join("campaign_daily");
    fs::create_dir_all(&ud).expect("mkdir user_daily");
    fs::create_dir_all(&cd).expect("mkdir campaign_daily");
    let camp_total = Mutex::new(vec![[0i64; 4]; (nc + 1) * nd]);
    par_items_then(
        nt,
        P,
        || PSt {
            map: PMap { keys: Vec::new(), vals: Vec::new(), bits: 0 },
            names: Vec::new(),
            ra: Vec::new(),
            rb: Vec::new(),
            ca_: Vec::new(),
            cb_: Vec::new(),
            cnt: Vec::new(),
            evs: Vec::new(),
            ccnt: Vec::new(),
            cls: Vec::new(),
            sess: Vec::new(),
            day_last: vec![0; nd],
            catsum: vec![0; ncat],
            touched: Vec::new(),
            open_lo: vec![0; nc + 1],
            open_hi: vec![0; nc + 1],
            open_t: Vec::new(),
            camp: vec![[0; 4]; (nc + 1) * nd],
            rows: Vec::new(),
        },
        |st, oi| {
            let p = order[oi];
            st.map.reset(12);
            st.names.clear();
            st.ra.clear();
            // surviving events with their person id
            for (s0, blk) in blocks(ea, &e2w, p) {
                for (i, e) in blk.iter().enumerate() {
                    let g = s0 as usize + i;
                    if (losers[g >> 6].load(Ordering::Relaxed) >> (g & 63)) & 1 != 0 {
                        continue;
                    }
                    let n = st.names.len() as u32;
                    let id = st.map.get_or(e.nh & LOW55, n);
                    if id == n {
                        st.names.push(e.name);
                        if st.names.len() * 2 > 1 << st.map.bits {
                            st.map.grow();
                        }
                    }
                    st.ra.push(R { ts: e.ts, pid: id, pc: e.pc, cathi: (e.nh >> 55) as u32 });
                }
            }
            let np = st.names.len();
            if np == 0 {
                return;
            }
            // by ts, then stably by person
            radix(&mut st.ra, &mut st.rb, |r| r.ts);
            st.cnt.clear();
            st.cnt.resize(np + 1, 0);
            for r in &st.ra {
                st.cnt[r.pid as usize + 1] += 1;
            }
            for i in 0..np {
                st.cnt[i + 1] += st.cnt[i];
            }
            let ne = st.ra.len();
            st.evs.clear();
            st.evs.reserve(ne);
            unsafe { st.evs.set_len(ne) };
            {
                let pos = &mut st.ccnt; // scratch
                pos.clear();
                pos.extend_from_slice(&st.cnt[..np]);
                for r in &st.ra {
                    let i = pos[r.pid as usize];
                    pos[r.pid as usize] = i + 1;
                    unsafe {
                        *st.evs.get_unchecked_mut(i as usize) =
                            Ev { ts: r.ts, price: r.pc & 0xFF_FFFF, cat: ((r.pc >> 24) | (r.cathi << 8)) as u16 };
                    }
                }
            }
            // clicks of these persons: by ts, then stably by person
            st.ca_.clear();
            for (_, blk) in blocks(ca, &c2w, p) {
                for c in blk {
                    let id = st.map.get(c.nh & LOW55);
                    if id != u32::MAX {
                        st.ca_.push(CR { ts: c.ts, pid: id, camp: c.camp, cid: c.cid });
                    }
                }
            }
            radix(&mut st.ca_, &mut st.cb_, |c| c.ts);
            st.ccnt.clear();
            st.ccnt.resize(np + 1, 0);
            for c in &st.ca_ {
                st.ccnt[c.pid as usize + 1] += 1;
            }
            for i in 0..np {
                st.ccnt[i + 1] += st.ccnt[i];
            }
            let ncl = st.ca_.len();
            st.cls.clear();
            st.cls.reserve(ncl);
            unsafe { st.cls.set_len(ncl) };
            {
                let mut pos: Vec<u32> = st.ccnt[..np].to_vec();
                for c in &st.ca_ {
                    let i = pos[c.pid as usize];
                    pos[c.pid as usize] = i + 1;
                    unsafe { *st.cls.get_unchecked_mut(i as usize) = Cl { ts: c.ts, camp: c.camp, cid: c.cid } };
                }
            }

            // per person
            for person in 0..np {
                let name = st.names[person];
                let evs = &st.evs[st.cnt[person] as usize..st.cnt[person + 1] as usize];

                // sessionize + user_daily
                st.sess.clear();
                let mut cur_day: i32 = -1;
                let (mut d_sess, mut d_ev, mut d_rev) = (0u32, 0u32, 0i64);
                macro_rules! flush_day {
                    () => {
                        if cur_day >= 0 {
                            let mut top: i32 = -1;
                            let mut best = 0i64;
                            for &c in &st.touched {
                                let v = st.catsum[c as usize];
                                if top < 0 || v > best || (v == best && cat_rank[c as usize] < cat_rank[top as usize]) {
                                    top = c as i32;
                                    best = v;
                                }
                                st.catsum[c as usize] = 0;
                            }
                            st.touched.clear();
                            st.rows.push(URow { name, day: d0 as i32 + cur_day, sessions: d_sess, events: d_ev, rev: d_rev, top });
                        }
                    };
                }
                let n = evs.len();
                let mut k = 0;
                while k < n {
                    let start = evs[k].ts;
                    let day = day_of(start);
                    if day as i32 != cur_day {
                        flush_day!();
                        cur_day = day as i32;
                        d_sess = 0;
                        d_ev = 0;
                        d_rev = 0;
                    }
                    let mut last = start;
                    let mut rev = 0i64;
                    let mut purch = false;
                    let k0 = k;
                    while k < n && evs[k].ts - last <= GAP {
                        let e = evs[k];
                        last = e.ts;
                        if e.price != 0 {
                            rev += e.price as i64;
                            purch = true;
                            let c = e.cat as usize;
                            if st.catsum[c] == 0 {
                                st.touched.push(e.cat);
                            }
                            st.catsum[c] += e.price as i64;
                        }
                        k += 1;
                    }
                    d_sess += 1;
                    d_ev += (k - k0) as u32;
                    d_rev += rev;
                    st.sess.push(Sess { start, rev, purch, day });
                }
                flush_day!();

                // credit: clicks are in ts order; order equal-ts runs by click_id
                let cls = &mut st.cls[st.ccnt[person] as usize..st.ccnt[person + 1] as usize];
                let m = cls.len();
                let mut i = 1;
                while i < m {
                    if cls[i].ts == cls[i - 1].ts {
                        let a = i - 1;
                        let mut b = i + 1;
                        while b < m && cls[b].ts == cls[a].ts {
                            b += 1;
                        }
                        cls[a..b].sort_unstable_by(|x, y| x.cid.get().cmp(y.cid.get()));
                        i = b;
                    } else {
                        i += 1;
                    }
                }
                let (mut j, mut gs) = (0usize, 0usize);
                for s in &st.sess {
                    while j < m && cls[j].ts <= s.start {
                        if j == 0 || cls[j].ts != cls[j - 1].ts {
                            gs = j;
                        }
                        j += 1;
                    }
                    let camp = if j > 0 && (s.start - cls[gs].ts) as i64 <= WEEK { cls[gs].camp } else { organic };
                    let c = &mut st.camp[camp as usize * nd + s.day as usize];
                    c[0] += 1;
                    c[1] += s.purch as i64;
                    c[2] += s.rev;
                }

                // assists: clicks in ts order reach session ranges [lo, hi) that
                // only move forward; per campaign, merge overlapping ranges
                if m > 0 {
                    for (i, s) in st.sess.iter().enumerate() {
                        st.day_last[s.day as usize] = i as u32;
                    }
                    let sess = &st.sess;
                    let ns = sess.len();
                    let day_last = &st.day_last;
                    let campst = &mut st.camp;
                    let flush = |campst: &mut Vec<[i64; 4]>, camp: u32, lo: usize, hi: usize| {
                        // sessions lo..hi (exclusive), spread over their days
                        let mut k = lo;
                        while k < hi {
                            let d = sess[k].day as usize;
                            let end = (day_last[d] as usize + 1).min(hi);
                            campst[camp as usize * nd + d][3] += (end - k) as i64;
                            k = end;
                        }
                    };
                    let (mut lo, mut hi) = (0usize, 0usize);
                    st.open_t.clear();
                    for c in cls.iter() {
                        while lo < ns && sess[lo].start < c.ts {
                            lo += 1;
                        }
                        while hi < ns && (sess[hi].start as i64) <= c.ts as i64 + WEEK {
                            hi += 1;
                        }
                        if lo >= hi {
                            continue;
                        }
                        let cp = c.camp as usize;
                        let oh = st.open_hi[cp] as usize;
                        if oh == 0 {
                            st.open_lo[cp] = lo as u32;
                            st.open_hi[cp] = hi as u32;
                            st.open_t.push(c.camp);
                        } else if lo <= oh {
                            st.open_hi[cp] = hi as u32;
                        } else {
                            flush(campst, c.camp, st.open_lo[cp] as usize, oh);
                            st.open_lo[cp] = lo as u32;
                            st.open_hi[cp] = hi as u32;
                        }
                    }
                    for &cp in &st.open_t {
                        flush(campst, cp, st.open_lo[cp as usize] as usize, st.open_hi[cp as usize] as usize);
                        st.open_hi[cp as usize] = 0;
                    }
                }
            }
        },
        // as soon as this worker is out of partitions: its user_daily part
        // file and its campaign counters
        |st, w| {
            {
                let mut tot = camp_total.lock().unwrap();
                for (a, b) in tot.iter_mut().zip(st.camp.iter()) {
                    for x in 0..4 {
                        a[x] += b[x];
                    }
                }
            }
            if st.rows.is_empty() && w > 0 {
                return;
            }
            let r = &st.rows;
            output::write(
                &ud.join(format!("part-{w:03}.parquet")),
                &[
                    ("user_id", Col::Str(r.iter().map(|x| x.name.get()).collect())),
                    ("day", Col::Date(r.iter().map(|x| x.day).collect())),
                    ("sessions", Col::I64(r.iter().map(|x| x.sessions as i64).collect())),
                    ("events", Col::I64(r.iter().map(|x| x.events as i64).collect())),
                    ("revenue_cents", Col::I64(r.iter().map(|x| x.rev).collect())),
                    (
                        "top_category",
                        Col::OptStr(r.iter().map(|x| if x.top < 0 { None } else { Some(cat_names[x.top as usize]) }).collect()),
                    ),
                ],
            );
        },
    );
    lap("stage 4: sessions built, user_daily written");

    // ---- stage 5: campaign_daily (user_daily is written by stage 4's workers)
    let tot = camp_total.into_inner().unwrap();
    let (mut c_id, mut c_ch, mut c_day) = (Vec::new(), Vec::new(), Vec::new());
    let (mut c_s, mut c_p, mut c_r, mut c_a) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for c in 0..=nc {
        for d in 0..nd {
            let v = tot[c * nd + d];
            if v[0] == 0 && v[3] == 0 {
                continue;
            }
            let (id, ch): (&[u8], &[u8]) = if c == nc { (b"organic", b"organic") } else { (camp_ids[c], channels[c]) };
            c_id.push(id);
            c_ch.push(ch);
            c_day.push((d0 + d as i64) as i32);
            c_s.push(v[0]);
            c_p.push(v[1]);
            c_r.push(v[2]);
            c_a.push(if c == nc { 0 } else { v[3] });
        }
    }
    output::write(
        &cd.join("part-000.parquet"),
        &[
            ("campaign_id", Col::Str(c_id)),
            ("channel", Col::Str(c_ch)),
            ("day", Col::Date(c_day)),
            ("sessions", Col::I64(c_s)),
            ("purchasing_sessions", Col::I64(c_p)),
            ("revenue_cents", Col::I64(c_r)),
            ("assisted_sessions", Col::I64(c_a)),
        ],
    );
    lap("done");
    // Skip freeing gigabytes of buffers.
    std::process::exit(0);
}
