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
use std::hash::{BuildHasherDefault, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::thread;
use std::time::Instant;

use output::Col;
use parse::{for_each_line, parse_price, parse_ts, DAY};

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

#[inline(always)]
fn h64(b: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64(b)
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
    // MAP_POPULATE (Linux): map every page up front in one kernel call
    // instead of taking a page fault per 4 KB while parsing.
    let m = unsafe { memmap2::MmapOptions::new().populate().map(&f) }.expect("mmap");
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

/// Linked IDs, keyed by the 128-bit hash of the ID (collisions at 128 bits
/// are not a practical concern), in a flat open-addressing table so lookups
/// touch one cache line and can be prefetched.
#[derive(Clone, Copy, Default)]
#[repr(C)]
struct LEnt {
    lo: u64,
    hi: u64,
    name: S,
    nlo: u64,
    nhi: u64,
}
impl Default for S {
    fn default() -> S {
        S(0)
    }
}

/// Linked-ID lookup table, split into LP regions by the top bits of the
/// ID hash so it can be built in parallel; open addressing inside a region.
struct Links {
    region: usize,
    tab: Vec<LEnt>,
    n: usize,
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
    fn start(&self, lo: u64) -> usize {
        (lo >> (64 - LB)) as usize * self.region + (((lo as u32 as u64) * self.region as u64) >> 32) as usize
    }
    #[inline(always)]
    fn prefetch(&self, lo: u64) {
        if !self.tab.is_empty() {
            prefetch(unsafe { self.tab.as_ptr().add(self.start(lo)) });
        }
    }
    /// The person (name, 128-bit name hash) of a user given its 128-bit hash.
    #[inline(always)]
    fn resolve(&self, user: S, lo: u64, hi: u64) -> (S, u64, u64) {
        if self.tab.is_empty() {
            return (user, lo, hi);
        }
        let base = (lo >> (64 - LB)) as usize * self.region;
        let mut s = self.start(lo);
        loop {
            let e = unsafe { self.tab.get_unchecked(s) };
            if e.lo == lo && e.hi == hi {
                return (e.name, e.nlo, e.nhi);
            }
            if e.lo == 0 && e.hi == 0 {
                return (user, lo, hi);
            }
            s += 1;
            if s == base + self.region {
                s = base;
            }
        }
    }
}

#[inline(always)]
fn h128(b: &[u8]) -> (u64, u64) {
    let h = xxhash_rust::xxh3::xxh3_128(b);
    (h as u64, (h >> 64) as u64)
}

fn find(parent: &mut [u32], mut x: u32) -> u32 {
    while parent[x as usize] != x {
        let p = parent[x as usize];
        parent[x as usize] = parent[p as usize];
        x = p;
    }
    x
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
    let a: Vec<A> = par_items(
        nt,
        ch.len(),
        || A { parts: (0..LP).map(|_| Vec::new()).collect(), npairs: 0 },
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
        return Links { region: 0, tab: Vec::new(), n: 0 };
    }
    let gid = |packed: u32| off[(packed >> 24) as usize] + (packed & 0xFF_FFFF);
    let id_of = |g: u32| -> &(S, u64, u64, u64) {
        let p = off.partition_point(|&o| o <= g) - 1;
        &regions[p][(g - off[p]) as usize]
    };
    // Union-find; parent[x] <= x always holds, so one ascending pass flattens it.
    let mut parent: Vec<u32> = (0..n as u32).collect();
    for w in &ep_ids {
        for pair in w.chunks_exact(2) {
            let ra = find(&mut parent, gid(pair[0]));
            let rb = find(&mut parent, gid(pair[1]));
            if ra != rb {
                parent[ra.max(rb) as usize] = ra.min(rb);
            }
        }
    }
    for i in 0..n {
        parent[i] = parent[parent[i] as usize];
    }
    // Smallest ID per person, as (key8, global id).
    let mut flat: Vec<(S, u64, u64, u64)> = Vec::with_capacity(n);
    for r in &regions {
        flat.extend_from_slice(r);
    }
    let _ = id_of;
    let mut best: Vec<u32> = (0..n as u32).collect();
    for i in 0..n {
        let r = parent[i] as usize;
        let b = best[r] as usize;
        let (x, y) = (&flat[i], &flat[b]);
        if x.3 < y.3 || (x.3 == y.3 && x.0.get() < y.0.get()) {
            best[r] = i as u32;
        }
    }
    // Lookup table.
    let max = regions.iter().map(|r| r.len()).max().unwrap();
    let region = max * 3 / 2 + 8;
    let mut tab = vec![LEnt::default(); LP * region];
    let tp = SendPtr(tab.as_mut_ptr());
    let links = Links { region, tab: Vec::new(), n };
    par_items(nt, LP, || (), |_, p| {
        let base = p * region;
        for (local, &(s, lo, hi, _)) in regions[p].iter().enumerate() {
            let _ = s;
            let g = off[p] as usize + local;
            let name = flat[best[parent[g] as usize] as usize];
            let mut slot = links.start(lo);
            unsafe {
                while (*tp.at(slot)).lo != 0 || (*tp.at(slot)).hi != 0 {
                    slot += 1;
                    if slot == base + region {
                        slot = base;
                    }
                }
                *tp.at(slot) = LEnt { lo, hi, name: name.0, nlo: name.1, nhi: name.2 };
            }
        }
    });
    Links { tab, ..links }
}

// ---------------------------------------------------------------------------
// Records

/// A valid event line waiting for its person to be resolved.
#[derive(Clone, Copy)]
struct Pend {
    h: u64,
    eid: S,
    ingest: i64,
    user: S,
    ulo: u64,
    uhi: u64,
    ts: u32,
    price: u32,
    cat: u16,
    typ: u8,
}

/// Dedup record of a parsed event line: where its E2 lives, or NONE.
#[derive(Clone, Copy)]
#[repr(C, packed(4))]
struct D {
    h: u64,
    eid: S,
    loc: u64,
    /// ingest_ts in seconds since `base` (ingest_ts is always F1: whole seconds)
    ingest: i32,
}
const NONE: u64 = u64::MAX;

/// A surviving event, keyed by person.
#[derive(Clone, Copy)]
struct E2 {
    nh: u64,
    name: S,
    nh2: u32,
    ts: u32,
    price: u32,
    cat: u16,
    typ: u8,
}

/// A valid click, keyed by person.
#[derive(Clone, Copy)]
struct C2 {
    nh: u64,
    cid: S,
    nh2: u32,
    ts: u32,
    camp: u32,
}

#[derive(Clone, Copy)]
struct Ev {
    ts: u32,
    price: u32,
    cat: u16,
    typ: u8,
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

type Parts<T> = Vec<Vec<T>>;

fn new_parts<T>(cap: usize) -> Parts<T> {
    (0..P).map(|_| Vec::with_capacity(cap)).collect()
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
    let mut products: hashbrown::HashMap<&'static [u8], u16> = hashbrown::HashMap::new();
    for_each_line::<2, _>(body(map_file(&data.join("products.csv"))), |f| {
        let n = cat_names.len() as u16;
        let c = *cat_ids.entry(f[1]).or_insert(n);
        if c == n {
            cat_names.push(f[1]);
        }
        products.entry(f[0]).or_insert(c);
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
    let mut camps: hashbrown::HashMap<&'static [u8], u32> = hashbrown::HashMap::new();
    for_each_line::<2, _>(body(map_file(&data.join("campaigns.csv"))), |f| {
        if !camps.contains_key(f[0]) {
            camps.insert(f[0], camp_ids.len() as u32);
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
    let links = build_links(&ln_files, nt);
    lap(&format!("links: {} linked IDs", links.n));

    // ---- stage 1: parse events. Valid events go straight to their person
    // partition (E2); every parsed line also leaves a compact dedup record
    // (D) in its event_id partition pointing at its E2 (or NONE if invalid).
    let ingest_s = |ms: i64| (ms - base).div_euclid(1000).clamp(i32::MIN as i64, i32::MAX as i64) as i32;
    let ev_chunks = chunks(&ev_files, 2 << 20);
    let cap1 = (ev_bytes / 70 / nt / P) * 9 / 8 + 16;
    let cl_chunks = chunks(&cl_files, 2 << 20);
    let cap3 = (cl_bytes / 45 / nt / P) * 9 / 8 + 16;
    struct ParseSt {
        t: usize,
        ev: Parts<E2>,
        dd: Parts<D>,
        pend: Vec<Pend>,
        cl: Parts<C2>,
        cpend: Vec<(S, u64, u64, S, u32, u32)>,
    }
    let worker = AtomicUsize::new(0);
    let st1: Vec<ParseSt> = par_items(
        nt,
        ev_chunks.len() + cl_chunks.len(),
        || ParseSt {
            t: worker.fetch_add(1, Ordering::Relaxed),
            ev: new_parts(cap1),
            dd: new_parts(cap1),
            pend: Vec::new(),
            cl: new_parts(cap3),
            cpend: Vec::new(),
        },
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
                    let Some(&camp) = camps.get(f[2]) else { return };
                    let (lo, hi) = h128(f[1]);
                    cpend.push((S::new(f[1]), lo, hi, S::new(f[0]), (ts - base) as u32, camp));
                });
                const AHEAD: usize = 12;
                for k in 0..st.cpend.len() {
                    if let Some(a) = st.cpend.get(k + AHEAD) {
                        links.prefetch(a.1);
                    }
                    let (user, lo, hi, cid, ts, camp) = st.cpend[k];
                    let (_, nh, nh2) = links.resolve(user, lo, hi);
                    st.cl[part_of(nh)].push(C2 { nh, nh2: nh2 as u32, cid, ts, camp });
                }
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
                let Some(ingest) = parse_ts(f[3]) else { return };
                let h = h64(eid);
                let mut valid = !f[1].is_empty() && !f[5].is_empty();
                let typ: u8 = match f[4] {
                    b"view" => 0,
                    b"cart" => 1,
                    b"purchase" => 2,
                    _ => {
                        valid = false;
                        3
                    }
                };
                let mut ts_rel = 0u32;
                if valid {
                    match parse_ts(f[2]) {
                        Some(ts) if ts >= ws && ts < we && ingest - ts <= DAY => ts_rel = (ts - base) as u32,
                        _ => valid = false,
                    }
                }
                let mut price = 0u32;
                let mut cat = 0u16;
                if valid && typ == 2 {
                    match parse_price(f[6]) {
                        Some(p) => {
                            price = p;
                            cat = *products.get(f[5]).unwrap_or(&unknown);
                        }
                        None => valid = false,
                    }
                }
                if !valid {
                    dd[part_of(h)].push(D { h, eid: S::new(eid), ingest: ingest_s(ingest), loc: NONE });
                    return;
                }
                let (ulo, uhi) = h128(f[1]);
                pend.push(Pend { h, eid: S::new(eid), ingest, user: S::new(f[1]), ulo, uhi, ts: ts_rel, price, cat, typ });
            });
            const AHEAD: usize = 12;
            for k in 0..st.pend.len() {
                if let Some(a) = st.pend.get(k + AHEAD) {
                    links.prefetch(a.ulo);
                }
                let r = st.pend[k];
                let (name, nh, nh2) = links.resolve(r.user, r.ulo, r.uhi);
                let p = part_of(nh);
                let loc = ((st.t as u64) << 56) | ((p as u64) << 40) | st.ev[p].len() as u64;
                st.ev[p].push(E2 { nh, nh2: nh2 as u32, name, ts: r.ts, price: r.price, cat: r.cat, typ: r.typ });
                st.dd[part_of(r.h)].push(D { h: r.h, eid: r.eid, ingest: ingest_s(r.ingest), loc });
            }
        },
    );
    let mut st1 = st1;
    st1.sort_by_key(|s| s.t);
    let mut e2: Vec<Parts<E2>> = Vec::new();
    let mut dd: Vec<Parts<D>> = Vec::new();
    let mut c2: Vec<Parts<C2>> = Vec::new();
    for s in st1 {
        e2.push(s.ev);
        dd.push(s.dd);
        c2.push(s.cl);
    }
    lap("stage 1: events and clicks parsed");

    // ---- stage 2: dedup by event_id; mark losing copies in per-partition bitmaps
    let losers: Vec<Vec<Vec<AtomicU64>>> = e2
        .iter()
        .map(|w| w.iter().map(|v| (0..v.len() / 64 + 1).map(|_| AtomicU64::new(0)).collect()).collect())
        .collect();
    let lose = |loc: u64| {
        if loc != NONE {
            let (t, p, i) = ((loc >> 56) as usize, ((loc >> 40) & 0xFFFF) as usize, (loc & 0xFF_FFFF_FFFF) as usize);
            losers[t][p][i >> 6].fetch_or(1 << (i & 63), Ordering::Relaxed);
        }
    };
    const TB: u32 = 26; // record ref = thread << TB | index
    assert!(nt < 64);
    let n1: usize = dd.iter().map(|w| w.iter().map(|v| v.len()).sum::<usize>()).sum();
    par_items(nt, P, Vec::<(u64, u32)>::new, |tab, p| {
        let srcs: Vec<&[D]> = dd.iter().map(|w| &w[p][..]).collect();
        let n: usize = srcs.iter().map(|v| v.len()).sum();
        if n == 0 {
            return;
        }
        let get = |g: u32| unsafe { srcs.get_unchecked((g >> TB) as usize).get_unchecked((g & ((1 << TB) - 1)) as usize) };
        let size = (n * 2).next_power_of_two();
        let mask = size - 1;
        tab.clear();
        tab.resize(size, (0, u32::MAX));
        for (t, v) in srcs.iter().enumerate() {
            assert!(v.len() < 1 << TB);
            for (i, r) in v.iter().enumerate() {
                let g = ((t as u32) << TB) | i as u32;
                let mut s = (r.h as usize) & mask;
                loop {
                    let (ch, cg) = unsafe { *tab.get_unchecked(s) };
                    if cg == u32::MAX {
                        tab[s] = (r.h, g);
                        break;
                    }
                    if ch == r.h {
                        let c = get(cg);
                        if c.eid.get() == r.eid.get() {
                            // Smallest ingest_ts wins; equal ones are identical copies.
                            if r.ingest < c.ingest {
                                lose(c.loc);
                                tab[s].1 = g;
                            } else {
                                lose(r.loc);
                            }
                            break;
                        }
                    }
                    s = (s + 1) & mask;
                }
            }
        }
    });
    let freer = thread::spawn(move || drop(dd));
    lap(&format!("stage 2: {} event lines deduped", n1));

    // ---- stage 4: per person partition
    let mut order: Vec<usize> = (0..P).collect();
    let psize = |p: usize| e2.iter().map(|w| w[p].len()).sum::<usize>() + c2.iter().map(|w| w[p].len()).sum::<usize>();
    let sizes: Vec<usize> = (0..P).map(psize).collect();
    order.sort_by_key(|&p| std::cmp::Reverse(sizes[p]));

    struct PSt {
        map: HMap128,
        names: Vec<S>,
        pid: Vec<u32>,
        cnt: Vec<u32>,
        evs: Vec<Ev>,
        ccnt: Vec<u32>,
        cls: Vec<Cl>,
        sess: Vec<Sess>,
        starts: Vec<u32>,
        ca: Vec<(u32, u32)>,
        day_last: Vec<u32>,
        catsum: Vec<i64>,
        touched: Vec<u16>,
        camp: Vec<[i64; 4]>,
        rows: Vec<URow>,
    }
    let ncat = cat_names.len();
    let states = par_items(
        nt,
        P,
        || PSt {
            map: HMap128::default(),
            names: Vec::new(),
            pid: Vec::new(),
            cnt: Vec::new(),
            evs: Vec::new(),
            ccnt: Vec::new(),
            cls: Vec::new(),
            sess: Vec::new(),
            starts: Vec::new(),
            ca: Vec::new(),
            day_last: vec![0; nd],
            catsum: vec![0; ncat],
            touched: Vec::new(),
            camp: vec![[0; 4]; (nc + 1) * nd],
            rows: Vec::new(),
        },
        |st, oi| {
            let p = order[oi];
            st.map.clear();
            st.names.clear();
            st.pid.clear();
            // group events by person
            for (t, w) in e2.iter().enumerate() {
                let lb = &losers[t][p];
                for (i, e) in w[p].iter().enumerate() {
                    if (lb[i >> 6].load(Ordering::Relaxed) >> (i & 63)) & 1 != 0 {
                        st.pid.push(u32::MAX);
                        continue;
                    }
                    let n = st.names.len() as u32;
                    let id = *st.map.entry((e.nh, e.nh2 as u64)).or_insert(n);
                    if id == n {
                        st.names.push(e.name);
                    }
                    st.pid.push(id);
                }
            }
            let np = st.names.len();
            if np == 0 {
                return;
            }
            // counting sort of events by person
            st.cnt.clear();
            st.cnt.resize(np + 1, 0);
            for &id in &st.pid {
                if id != u32::MAX {
                    st.cnt[id as usize + 1] += 1;
                }
            }
            for i in 0..np {
                st.cnt[i + 1] += st.cnt[i];
            }
            let ne = st.cnt[np] as usize;
            st.evs.clear();
            st.evs.reserve(ne);
            unsafe { st.evs.set_len(ne) };
            {
                let mut pos: Vec<u32> = st.cnt[..np].to_vec();
                let mut k = 0;
                for w in &e2 {
                    for e in &w[p] {
                        let id = st.pid[k] as usize;
                        k += 1;
                        if id == u32::MAX as usize {
                            continue;
                        }
                        st.evs[pos[id] as usize] = Ev { ts: e.ts, price: e.price, cat: e.cat, typ: e.typ };
                        pos[id] += 1;
                    }
                }
            }
            // clicks of these persons, counting sort
            st.pid.clear();
            for w in &c2 {
                for c in &w[p] {
                    st.pid.push(st.map.get(&(c.nh, c.nh2 as u64)).copied().unwrap_or(u32::MAX));
                }
            }
            st.ccnt.clear();
            st.ccnt.resize(np + 1, 0);
            for &id in &st.pid {
                if id != u32::MAX {
                    st.ccnt[id as usize + 1] += 1;
                }
            }
            for i in 0..np {
                st.ccnt[i + 1] += st.ccnt[i];
            }
            let ncl = st.ccnt[np] as usize;
            st.cls.clear();
            st.cls.reserve(ncl);
            unsafe { st.cls.set_len(ncl) };
            {
                let mut pos: Vec<u32> = st.ccnt[..np].to_vec();
                let mut k = 0;
                for w in &c2 {
                    for c in &w[p] {
                        let id = st.pid[k];
                        k += 1;
                        if id != u32::MAX {
                            st.cls[pos[id as usize] as usize] = Cl { ts: c.ts, camp: c.camp, cid: c.cid };
                            pos[id as usize] += 1;
                        }
                    }
                }
            }

            // per person
            for person in 0..np {
                let name = st.names[person];
                let evs = &mut st.evs[st.cnt[person] as usize..st.cnt[person + 1] as usize];
                evs.sort_unstable_by_key(|e| e.ts);

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
                        if e.typ == 2 {
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

                // credit
                let cls = &mut st.cls[st.ccnt[person] as usize..st.ccnt[person + 1] as usize];
                if cls.len() > 1 {
                    cls.sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| a.cid.get().cmp(b.cid.get())));
                }
                let m = cls.len();
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

                // assists: for each campaign, the union of session ranges its clicks reach
                if m > 0 {
                    st.starts.clear();
                    st.starts.extend(st.sess.iter().map(|s| s.start));
                    for (i, s) in st.sess.iter().enumerate() {
                        st.day_last[s.day as usize] = i as u32;
                    }
                    st.ca.clear();
                    st.ca.extend(cls.iter().map(|c| (c.camp, c.ts)));
                    st.ca.sort_unstable();
                    let starts = &st.starts;
                    let sess = &st.sess;
                    let day_last = &st.day_last;
                    let campst = &mut st.camp;
                    let mut flush = |camp: u32, lo: usize, hi: usize| {
                        // sessions lo..hi (exclusive), spread over their days
                        let mut k = lo;
                        while k < hi {
                            let d = sess[k].day as usize;
                            let end = (day_last[d] as usize + 1).min(hi);
                            campst[camp as usize * nd + d][3] += (end - k) as i64;
                            k = end;
                        }
                    };
                    let mut i = 0;
                    while i < st.ca.len() {
                        let camp = st.ca[i].0;
                        let (mut rlo, mut rhi) = (0usize, 0usize); // current open range, empty
                        while i < st.ca.len() && st.ca[i].0 == camp {
                            let t = st.ca[i].1;
                            i += 1;
                            let lo = starts.partition_point(|&s| s < t);
                            let hi = starts.partition_point(|&s| (s as i64) <= t as i64 + WEEK);
                            if lo >= hi {
                                continue;
                            }
                            if rlo < rhi && lo <= rhi {
                                rhi = rhi.max(hi);
                            } else {
                                if rlo < rhi {
                                    flush(camp, rlo, rhi);
                                }
                                rlo = lo;
                                rhi = hi;
                            }
                        }
                        if rlo < rhi {
                            flush(camp, rlo, rhi);
                        }
                    }
                }
            }
        },
    );
    lap("stage 4: sessions built");
    drop(freer);

    // ---- stage 5: write
    let ud = out.join("user_daily");
    let cd = out.join("campaign_daily");
    fs::create_dir_all(&ud).expect("mkdir user_daily");
    fs::create_dir_all(&cd).expect("mkdir campaign_daily");
    let camp_total = Mutex::new(vec![[0i64; 4]; (nc + 1) * nd]);
    thread::scope(|sc| {
        for (w, st) in states.iter().enumerate() {
            let ud = &ud;
            let cat_names = &cat_names;
            let camp_total = &camp_total;
            sc.spawn(move || {
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
            });
        }
    });
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
