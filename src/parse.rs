//! CSV dialect, timestamps and prices, exactly as rules.md defines them.

pub const SECOND: i64 = 1_000;
pub const MINUTE: i64 = 60 * SECOND;
pub const HOUR: i64 = 60 * MINUTE;
pub const DAY: i64 = 24 * HOUR;

/// Calls `f` with the unquoted fields of every well-formed line of `buf`
/// (which must start at a line start and end at a line end or EOF).
/// A line is well formed when it has exactly `N` fields and every field is
/// either quote-free or a whole field wrapped in one pair of quotes.
///
/// Two passes over segments of up to SEG 64-byte blocks. A SIMD pass writes
/// comma and quote bit masks with running counts, and the positions of all
/// commas and newlines. The line pass then walks the newlines: prefix counts
/// give each line's first comma and its comma and quote counts in O(1), its
/// fields come straight from the comma positions, and quotes are stripped
/// without branches (valid iff the line's quote count is twice the number of
/// fields that start and end with a quote).
#[inline(always)]
pub fn for_each_line<'a, const N: usize, F: FnMut(&[&'a [u8]; N])>(buf: &'a [u8], mut f: F) {
    const SEG: usize = 2048;
    let len = buf.len();
    assert!(len < u32::MAX as usize - 128);
    let nblocks = len.div_ceil(64);
    let mut sc = SCRATCH.with(|c| c.take());
    let mut ls = 0usize;
    let mut seg = 0usize;
    let mut base = 0usize;
    while seg < nblocks {
        let end = (seg + SEG).min(nblocks);
        fill(buf, seg, end, &mut sc);
        base = seg * 64;
        for &le in sc.npos.iter() {
            let le = le as usize;
            if le < ls {
                continue; // before the segment's first line start: done already
            }
            line::<N, F>(buf, &sc, base, ls, le, &mut f);
            ls = le + 1;
        }
        if end == nblocks {
            break;
        }
        let next = ls >> 6;
        if next > seg {
            seg = next;
            continue;
        }
        // A line longer than a segment: handle it on its own.
        match memchr::memchr(b'\n', &buf[end * 64..]) {
            Some(k) => {
                let le = end * 64 + k;
                slow_line::<N, F>(&buf[ls..le], &mut f);
                ls = le + 1;
                seg = ls >> 6;
            }
            None => {
                slow_line::<N, F>(&buf[ls..], &mut f);
                ls = len;
                break;
            }
        }
    }
    if ls < len {
        // Last line without a newline (its blocks are in the last segment).
        line::<N, F>(buf, &sc, base, ls, len, &mut f);
    }
    SCRATCH.with(|c| c.set(sc));
}

#[derive(Clone, Copy, Default)]
#[repr(C)]
struct Blk {
    c: u64,
    q: u64,
    /// commas and quotes before this block, in the segment
    cc: u32,
    qc: u32,
}

#[derive(Default)]
struct Scratch {
    blks: Vec<Blk>,
    cpos: Vec<u32>,
    npos: Vec<u32>,
}

thread_local! {
    static SCRATCH: std::cell::Cell<Scratch> = const { std::cell::Cell::new(Scratch { blks: Vec::new(), cpos: Vec::new(), npos: Vec::new() }) };
}

/// Writes the positions of the set bits of `m` (plus `off`) at `out`;
/// returns how many. Writes up to 8 (or the count, if more) unconditionally.
#[inline(always)]
unsafe fn flatten(out: *mut u32, mut m: u64, off: u32) -> usize {
    let cnt = m.count_ones() as usize;
    for k in 0..8 {
        *out.add(k) = off + m.trailing_zeros();
        m &= m.wrapping_sub(1);
    }
    if cnt > 8 {
        for k in 8..cnt {
            *out.add(k) = off + m.trailing_zeros();
            m &= m - 1;
        }
    }
    cnt
}

/// Masks, running counts and comma/newline positions of blocks [seg, end),
/// plus one empty sentinel block.
#[inline(never)]
fn fill(buf: &[u8], seg: usize, end: usize, sc: &mut Scratch) {
    let nb = end - seg;
    sc.blks.clear();
    sc.blks.reserve(nb + 1);
    sc.cpos.clear();
    sc.cpos.reserve(nb * 64 + 16);
    sc.npos.clear();
    sc.npos.reserve(nb * 64 + 16);
    let (cp, np) = (sc.cpos.as_mut_ptr(), sc.npos.as_mut_ptr());
    let (mut cc, mut nc, mut qc) = (0usize, 0usize, 0u32);
    let len = buf.len();
    for b in seg..end {
        let off = b * 64;
        let (c, n, q) = if off + 64 <= len {
            unsafe { masks(buf.as_ptr().add(off)) }
        } else {
            let mut tail = [0u8; 64];
            tail[..len - off].copy_from_slice(&buf[off..]);
            unsafe { masks(tail.as_ptr()) }
        };
        sc.blks.push(Blk { c, q, cc: cc as u32, qc });
        unsafe {
            cc += flatten(cp.add(cc), c, off as u32);
            let mut m = n;
            let k = n.count_ones() as usize;
            *np.add(nc) = off as u32 + m.trailing_zeros();
            m &= m.wrapping_sub(1);
            *np.add(nc + 1) = off as u32 + m.trailing_zeros();
            m &= m.wrapping_sub(1);
            if k > 2 {
                for j in 2..k {
                    *np.add(nc + j) = off as u32 + m.trailing_zeros();
                    m &= m - 1;
                }
            }
            nc += k;
        }
        qc += q.count_ones();
    }
    sc.blks.push(Blk { c: 0, q: 0, cc: cc as u32, qc });
    unsafe {
        sc.cpos.set_len(cc);
        sc.npos.set_len(nc);
    }
}

#[inline(always)]
fn line<'a, const N: usize, F: FnMut(&[&'a [u8]; N])>(buf: &'a [u8], sc: &Scratch, base: usize, ls: usize, le: usize, f: &mut F) {
    if le == ls {
        return;
    }
    let at = |x: usize| unsafe { sc.blks.get_unchecked((x - base) >> 6) };
    let low = |x: usize| (1u64 << (x & 63)).wrapping_sub(1);
    let (a, b) = (at(ls), at(le));
    let r0 = (a.cc + (a.c & low(ls)).count_ones()) as usize;
    let r1 = (b.cc + (b.c & low(le)).count_ones()) as usize;
    if r1 - r0 != N - 1 {
        return;
    }
    let quotes = b.qc + (b.q & low(le)).count_ones() - a.qc - (a.q & low(ls)).count_ones();
    let mut bounds = [0usize; N];
    let cpos = unsafe { sc.cpos.get_unchecked(r0..r0 + N - 1) };
    for k in 0..N - 1 {
        bounds[k] = cpos[k] as usize;
    }
    bounds[N - 1] = le;
    let mut fields: [&'a [u8]; N] = [&[]; N];
    let mut s = ls;
    if quotes == 0 {
        for k in 0..N {
            fields[k] = unsafe { buf.get_unchecked(s..bounds[k]) };
            s = bounds[k] + 1;
        }
    } else if le < buf.len() {
        // every field start is at most le < len, so its first byte exists;
        // the byte before a field end exists unless the end is 0 (then the
        // field is empty and the byte is not used)
        let p = buf.as_ptr();
        let mut ok = 0usize;
        for k in 0..N {
            let e = bounds[k];
            let (first, lastb) = unsafe { (*p.add(s), *p.add(e.max(1) - 1)) };
            let q = ((e >= s + 2) & (first == b'"') & (lastb == b'"')) as usize;
            fields[k] = unsafe { std::slice::from_raw_parts(p.add(s + q), e - s - 2 * q) };
            ok += q;
            s = e + 1;
        }
        if 2 * ok as u32 != quotes {
            return;
        }
    } else {
        // the last line of the buffer, without a newline
        let mut ok = 0u32;
        let last = buf.len() - 1;
        for k in 0..N {
            let e = bounds[k];
            let first = unsafe { *buf.get_unchecked(s.min(last)) };
            let lastb = unsafe { *buf.get_unchecked(e.wrapping_sub(1).max(s).min(last)) };
            let q = ((e - s >= 2) & (first == b'"') & (lastb == b'"')) as usize;
            fields[k] = unsafe { buf.get_unchecked(s + q..e - q) };
            ok += 2 * q as u32;
            s = e + 1;
        }
        if ok != quotes {
            return;
        }
    }
    f(&fields);
}

/// One line (without its newline), the plain way.
#[cold]
fn slow_line<'a, const N: usize, F: FnMut(&[&'a [u8]; N])>(l: &'a [u8], f: &mut F) {
    if l.is_empty() {
        return;
    }
    let mut fields: [&'a [u8]; N] = [&[]; N];
    let mut nf = 0;
    for fld in l.split(|&c| c == b',') {
        if nf == N {
            return;
        }
        match unquote(fld) {
            Some(v) => fields[nf] = v,
            None => return,
        }
        nf += 1;
    }
    if nf == N {
        f(&fields);
    }
}

/// A field's value: quote-free as is, or one pair of quotes around it.
#[inline]
fn unquote(fld: &[u8]) -> Option<&[u8]> {
    let n = memchr::memchr_iter(b'"', fld).count();
    if n == 0 {
        Some(fld)
    } else if n == 2 && fld.len() >= 2 && fld[0] == b'"' && fld[fld.len() - 1] == b'"' {
        Some(&fld[1..fld.len() - 1])
    } else {
        None
    }
}

/// For the 64 bytes at `p`: (comma mask, newline mask, quote mask).
#[inline(always)]
unsafe fn masks(p: *const u8) -> (u64, u64, u64) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        use core::arch::x86_64::*;
        let comma = _mm256_set1_epi8(b',' as i8);
        let quote = _mm256_set1_epi8(b'"' as i8);
        let nl = _mm256_set1_epi8(b'\n' as i8);
        let a = _mm256_loadu_si256(p as *const __m256i);
        let b = _mm256_loadu_si256(p.add(32) as *const __m256i);
        let mm = |x: __m256i, y: __m256i| -> u64 {
            (_mm256_movemask_epi8(x) as u32 as u64) | ((_mm256_movemask_epi8(y) as u32 as u64) << 32)
        };
        let nla = _mm256_cmpeq_epi8(a, nl);
        let nlb = _mm256_cmpeq_epi8(b, nl);
        let n = mm(nla, nlb);
        let c = mm(_mm256_cmpeq_epi8(a, comma), _mm256_cmpeq_epi8(b, comma));
        let q = mm(_mm256_cmpeq_epi8(a, quote), _mm256_cmpeq_epi8(b, quote));
        return (c, n, q);
    }
    #[cfg(target_arch = "aarch64")]
    {
        use core::arch::aarch64::*;
        const BITS: [u8; 16] = [1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128];
        let bits = vld1q_u8(BITS.as_ptr());
        let v = [vld1q_u8(p), vld1q_u8(p.add(16)), vld1q_u8(p.add(32)), vld1q_u8(p.add(48))];
        let mask = |c: u8| -> u64 {
            let cv = vdupq_n_u8(c);
            let m0 = vandq_u8(vceqq_u8(v[0], cv), bits);
            let m1 = vandq_u8(vceqq_u8(v[1], cv), bits);
            let m2 = vandq_u8(vceqq_u8(v[2], cv), bits);
            let m3 = vandq_u8(vceqq_u8(v[3], cv), bits);
            let s = vpaddq_u8(vpaddq_u8(m0, m1), vpaddq_u8(m2, m3));
            let s = vpaddq_u8(s, s);
            vgetq_lane_u64(vreinterpretq_u64_u8(s), 0)
        };
        let n = mask(b'\n');
        return (mask(b','), n, mask(b'"'));
    }
    #[allow(unreachable_code)]
    {
        let (mut c, mut n, mut q) = (0u64, 0u64, 0u64);
        for i in 0..64 {
            let ch = *p.add(i);
            c |= ((ch == b',') as u64) << i;
            n |= ((ch == b'\n') as u64) << i;
            q |= ((ch == b'"') as u64) << i;
        }
        (c, n, q)
    }
}

#[inline(always)]
fn d2(b: &[u8], i: usize) -> Option<i64> {
    let x = b[i].wrapping_sub(b'0');
    let y = b[i + 1].wrapping_sub(b'0');
    if x < 10 && y < 10 {
        Some((x * 10 + y) as i64)
    } else {
        None
    }
}

#[inline]
pub fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let yoe = year - era * 400;
    let doy = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

#[inline]
fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) {
                29
            } else {
                28
            }
        }
    }
}

/// `YYYY-MM-DDTHH:MM:SS` at the start of `b` (length checked by the caller).
#[inline(always)]
fn civil_ms(b: &[u8]) -> Option<i64> {
    if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let year = d2(b, 0)? * 100 + d2(b, 2)?;
    let month = d2(b, 5)?;
    let day = d2(b, 8)?;
    let hour = d2(b, 11)?;
    let minute = d2(b, 14)?;
    let second = d2(b, 17)?;
    if !(1..=12).contains(&month)
        || day < 1
        || day > days_in_month(year, month)
        || hour > 23
        || minute > 59
        || second > 59
    {
        return None;
    }
    Some(days_from_civil(year, month, day) * DAY + hour * HOUR + minute * MINUTE + second * SECOND)
}

/// Epoch milliseconds of a valid F1, F2 or F3 timestamp; `None` for anything
/// else. An F3 value that does not fit an i64 is returned as `None`, which is
/// fine because it lies outside every window. SWAR fast paths for F1, F2 and
/// 13-digit F3; everything else (and anything they reject) goes to the
/// plain parser, so they only need to be exact on what they accept.
#[inline(always)]
pub fn parse_ts(b: &[u8]) -> Option<i64> {
    let n = b.len();
    if n == 20 || n == 25 {
        if let Some(v) = civil_fast(b) {
            return Some(v);
        }
    } else if n == 13 {
        let a = u64::from_le_bytes(b[0..8].try_into().unwrap());
        let c = u64::from_le_bytes(b[5..13].try_into().unwrap());
        if digits(a, !0) && digits(c, !0) {
            return Some((parse8(a) * 100_000 + parse8(c) % 100_000) as i64);
        }
    }
    parse_ts_slow(b)
}

const ZEROS: u64 = 0x3030_3030_3030_3030;

/// `ingest_ts`, which the rules guarantee is a valid F1: only its shape and
/// the table range are checked (anything else goes to `parse_ts`).
#[inline(always)]
pub fn parse_ingest(b: &[u8]) -> Option<i64> {
    if b.len() != 20 {
        return parse_ts(b);
    }
    let lo = u64::from_le_bytes(b[0..8].try_into().unwrap()); // YYYY-MM-
    let mid = u64::from_le_bytes(b[8..16].try_into().unwrap()); // DDTHH:MM
    let t = u32::from_le_bytes(b[16..20].try_into().unwrap()) as u64; // :SSZ
    const DM_LO: u64 = 0x00FF_FF00_FFFF_FFFF;
    const DM_MID: u64 = 0xFFFF_00FF_FF00_FFFF;
    let l = (lo & DM_LO).wrapping_sub(ZEROS & DM_LO);
    let lp = l.wrapping_mul(10).wrapping_add(l >> 8);
    let m = (mid & DM_MID).wrapping_sub(ZEROS & DM_MID);
    let mp = m.wrapping_mul(10).wrapping_add(m >> 8);
    let year = (lp & 0xFF) * 100 + ((lp >> 16) & 0xFF);
    let month = (lp >> 40) & 0xFF;
    let yi = year.wrapping_sub(1900);
    let shape = ((lo & !DM_LO) ^ 0x2D00_002D_0000_0000) | ((mid & !DM_MID) ^ 0x0000_3A00_0054_0000) | ((t & 0xFF00_00FF) ^ 0x5A00_003A);
    if shape != 0 || yi >= 300 || month.wrapping_sub(1) >= 12 || !digits(lo, DM_LO) {
        return parse_ts(b);
    }
    let day = (mp & 0xFF) as i64;
    let hour = ((mp >> 24) & 0xFF) as i64;
    let minute = ((mp >> 48) & 0xFF) as i64;
    let second = (((t >> 8) & 0xFF) * 10 + ((t >> 16) & 0xFF)) as i64 - 528;
    let days = MONTHS[(yi * 12 + month - 1) as usize] as i64 + day - 1;
    Some(days * DAY + hour * HOUR + minute * MINUTE + second * SECOND)
}

/// Are the bytes of `x` selected by `m` (0xFF per byte) all ASCII digits?
#[inline(always)]
fn digits(x: u64, m: u64) -> bool {
    let hi = 0xF0F0_F0F0_F0F0_F0F0u64 & m;
    (x & hi) == (ZEROS & m) && (x.wrapping_add(0x0606_0606_0606_0606) & hi) == (ZEROS & m)
}

/// Value of 8 ASCII digits (first byte most significant).
#[inline(always)]
fn parse8(x: u64) -> u64 {
    let mut v = x - ZEROS;
    v = (v * 10) + (v >> 8);
    v = (((v & 0x0000_00FF_0000_00FF) * (100 + (1_000_000 << 32)))
        + (((v >> 16) & 0x0000_00FF_0000_00FF) * (1 + (10_000 << 32))))
        >> 32;
    v
}

/// Day numbers of the first of each month, 1900-01 to 2200-01:
/// MONTHS[(y - 1900) * 12 + (m - 1)].
const MONTHS: [i32; 300 * 12 + 1] = {
    let mut t = [0i32; 300 * 12 + 1];
    let mut i = 0;
    while i <= 300 * 12 {
        let (y, m) = (1900 + (i / 12) as i64, (i % 12) as i64 + 1);
        let yy = if m <= 2 { y - 1 } else { y };
        let era = yy.div_euclid(400);
        let yoe = yy - era * 400;
        let doy = (153 * ((m + 9) % 12) + 2) / 5;
        t[i] = (era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468) as i32;
        i += 1;
    }
    t
};

/// F1 (n == 20) or F2 (n == 25); None when not valid, not that shape, or
/// outside the years 1900 to 2199 (the plain parser then decides).
#[inline(always)]
fn civil_fast(b: &[u8]) -> Option<i64> {
    let n = b.len();
    let lo = u64::from_le_bytes(b[0..8].try_into().unwrap()); // YYYY-MM-
    let mid = u64::from_le_bytes(b[8..16].try_into().unwrap()); // DDTHH:MM
    let t = u32::from_le_bytes(b[16..20].try_into().unwrap()) as u64; // :SS?
    const DM_LO: u64 = 0x00FF_FF00_FFFF_FFFF;
    const SEP_LO: u64 = 0x2D00_002D_0000_0000;
    const DM_MID: u64 = 0xFFFF_00FF_FF00_FFFF;
    const SEP_MID: u64 = 0x0000_3A00_0054_0000;
    let seps = ((lo & !DM_LO) ^ SEP_LO) | ((mid & !DM_MID) ^ SEP_MID) | ((t & 0xFF) ^ 0x3A);
    if seps != 0 || !(digits(lo, DM_LO) & digits(mid, DM_MID) & digits(t, 0x00FF_FF00)) {
        return None;
    }
    // byte i of x*10 + (x >> 8) is 10 * digit i + digit i+1
    let l = (lo & DM_LO) - (ZEROS & DM_LO);
    let lp = l * 10 + (l >> 8);
    let m = (mid & DM_MID) - (ZEROS & DM_MID);
    let mp = m * 10 + (m >> 8);
    let year = (lp & 0xFF) * 100 + ((lp >> 16) & 0xFF);
    let month = (lp >> 40) & 0xFF;
    let day = mp & 0xFF;
    let hour = (mp >> 24) & 0xFF;
    let minute = (mp >> 48) & 0xFF;
    let second = ((t >> 8) & 0xFF) * 10 + ((t >> 16) & 0xFF) - 528;
    let yi = year.wrapping_sub(1900);
    if (yi >= 300) | (month.wrapping_sub(1) >= 12) | (hour > 23) | (minute > 59) | (second > 59) {
        return None;
    }
    let idx = (yi * 12 + month - 1) as usize;
    let (ms, me) = (MONTHS[idx] as i64, MONTHS[idx + 1] as i64);
    if (day.wrapping_sub(1) as i64) >= me - ms || day == 0 {
        return None;
    }
    let tag = (t >> 24) as u8;
    let mut off = 0i64;
    if n == 20 {
        if tag != b'Z' {
            return None;
        }
    } else {
        // +HH:MM / -HH:MM
        let o = u64::from_le_bytes(b[17..25].try_into().unwrap()); // SS?HH:MM
        if (tag != b'+' && tag != b'-') || (o >> 40) & 0xFF != 0x3A || !digits(o, 0xFFFF_00FF_FF00_0000) {
            return None;
        }
        let oh = (((o >> 24) & 0xFF) as i64 - 48) * 10 + (((o >> 32) & 0xFF) as i64 - 48);
        let om = (((o >> 48) & 0xFF) as i64 - 48) * 10 + (((o >> 56) & 0xFF) as i64 - 48);
        if oh > 23 || om > 59 {
            return None;
        }
        off = (oh * 60 + om) * MINUTE;
        if tag == b'+' {
            off = -off;
        }
    }
    let days = ms + day as i64 - 1;
    Some(days * DAY + hour as i64 * HOUR + minute as i64 * MINUTE + second as i64 * SECOND + off)
}

/// Epoch milliseconds of a valid F1, F2 or F3 timestamp; `None` for anything
/// else. An F3 value that does not fit an i64 is returned as `None`, which is
/// fine because it lies outside every window.
#[cold]
#[inline(never)]
fn parse_ts_slow(b: &[u8]) -> Option<i64> {
    let n = b.len();
    if n == 0 {
        return None;
    }
    if n == 20 && b[19] == b'Z' {
        return civil_ms(b);
    }
    if n == 25 && (b[19] == b'+' || b[19] == b'-') {
        let sign = if b[19] == b'+' { 1 } else { -1 };
        if b[22] != b':' {
            return None;
        }
        let oh = d2(b, 20)?;
        let om = d2(b, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        return Some(civil_ms(b)? - sign * (oh * HOUR + om * MINUTE));
    }
    // F3: digits only, leading zeros allowed.
    if !b.iter().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let mut i = 0;
    while i < n && b[i] == b'0' {
        i += 1;
    }
    if n - i > 19 {
        return None;
    }
    let mut v: u64 = 0;
    for &c in &b[i..] {
        v = v * 10 + (c - b'0') as u64;
    }
    if v > i64::MAX as u64 {
        None
    } else {
        Some(v as i64)
    }
}

/// A purchase price: ASCII digits only, value 1 to 10,000,000.
#[inline]
pub fn parse_price(b: &[u8]) -> Option<u32> {
    if b.is_empty() || !b.iter().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let mut i = 0;
    while i < b.len() && b[i] == b'0' {
        i += 1;
    }
    if b.len() - i > 8 {
        return None;
    }
    let mut v: u32 = 0;
    for &c in &b[i..] {
        v = v * 10 + (c - b'0') as u32;
    }
    if (1..=10_000_000).contains(&v) {
        Some(v)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn ts() {
        let f1 = parse_ts(b"2026-10-03T23:58:10Z").unwrap();
        assert_eq!(f1, 1791071890000);
        assert_eq!(parse_ts(b"2026-10-03T19:58:10-04:00"), Some(f1));
        assert_eq!(parse_ts(b"1791071890000"), Some(f1));
        assert_eq!(parse_ts(b"0001791071890000"), Some(f1));
        assert_eq!(parse_ts(b"00000000000000000000000000001791071890000"), Some(f1));
        assert_eq!(parse_ts(b"2026-10-03T23:58:10-00:00"), Some(f1));
        for bad in [
            &b""[..], b"2026-10-03T23:58:10z", b"2026-10-03t23:58:10Z", b"2026-10-03 23:58:10Z",
            b"2026-10-03T23:58:10", b"1791071890000.0", b"1.79e12", b"-5", b"2026-13-03T23:58:10Z",
            b"2026-02-30T23:58:10Z", b"2027-02-29T23:58:10Z", b"2026-10-03T24:58:10Z",
            b"2026-10-03T23:58:60Z", b"2026-10-03T23:58:10+24:00", b"2026-10-03T23:58:10+05:60",
            b"2026-10-03T23:58:10+0200", b"2026-10-03T23:58:10.000Z", b"99999999999999999999",
        ] {
            assert_eq!(parse_ts(bad), None, "{:?}", std::str::from_utf8(bad));
        }
        assert!(parse_ts(b"2028-02-29T00:00:00Z").is_some());
        assert_eq!(parse_ts(b"86400000"), Some(86400000));
    }
    #[test]
    fn ts_fuzz() {
        let mut state = 0x1234_5678_9ABC_DEF1u64;
        let mut rnd = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let seeds: [&[u8]; 8] = [
            b"2027-12-18T22:22:54Z", b"2027-12-12T12:21:38-06:00", b"2028-02-29T23:59:59+05:30", b"1829168499000",
            b"0000-01-01T00:00:00Z", b"9999-12-31T23:59:59Z", b"2100-02-28T00:00:00-23:59", b"0001829168499",
        ];
        let alpha = b"0123456789:-+TZ /09";
        for it in 0..2_000_000 {
            let mut v = seeds[it % seeds.len()].to_vec();
            let k = (rnd() % 3) as usize;
            for _ in 0..k {
                let i = (rnd() as usize) % v.len();
                v[i] = alpha[(rnd() as usize) % alpha.len()];
            }
            assert_eq!(parse_ts(&v), parse_ts_slow(&v), "{:?}", String::from_utf8_lossy(&v));
        }
        for i in 0..200_000u64 {
            let t = (i * 7_919_993_000) % 4_000_000_000_000;
            let secs = t / 1000;
            let (d, r) = ((secs / 86400) as i64, secs % 86400);
            // civil from days (inverse of days_from_civil)
            let z = d + 719_468;
            let era = z.div_euclid(146_097);
            let doe = z - era * 146_097;
            let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
            let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
            let mp = (5 * doy + 2) / 153;
            let dd = doy - (153 * mp + 2) / 5 + 1;
            let mm = if mp < 10 { mp + 3 } else { mp - 9 };
            let yy = yoe + era * 400 + (mm <= 2) as i64;
            let s = format!("{yy:04}-{mm:02}-{dd:02}T{:02}:{:02}:{:02}Z", r / 3600, r / 60 % 60, r % 60);
            assert_eq!(parse_ingest(s.as_bytes()), parse_ts_slow(s.as_bytes()), "{s}");
            assert_eq!(parse_ingest(s.as_bytes()), Some((secs * 1000) as i64), "{s}");
        }
        for y in [0i64, 1, 4, 100, 400, 1600, 1900, 1970, 2000, 2024, 2027, 2100, 9999] {
            for mo in 1..=12 {
                for d in [1, 28, 29, 30, 31] {
                    let s = format!("{y:04}-{mo:02}-{d:02}T13:07:59Z");
                    assert_eq!(parse_ts(s.as_bytes()), parse_ts_slow(s.as_bytes()), "{s}");
                }
            }
        }
    }

    #[test]
    fn lines() {
        let mut out = Vec::new();
        for_each_line::<2, _>(b"a,b\n\"c\",\"\"\nx\n,\n\"a,b\"\na\"b,c\nq,r", |f| {
            out.push((f[0].to_vec(), f[1].to_vec()))
        });
        assert_eq!(
            out,
            vec![
                (b"a".to_vec(), b"b".to_vec()),
                (b"c".to_vec(), b"".to_vec()),
                (b"".to_vec(), b"".to_vec()),
                (b"q".to_vec(), b"r".to_vec())
            ]
        );
    }

    /// Naive reading of the dialect, for comparison.
    fn naive<const N: usize>(buf: &[u8]) -> Vec<Vec<Vec<u8>>> {
        let mut out = Vec::new();
        let mut lines: Vec<&[u8]> = buf.split(|&c| c == b'\n').collect();
        if buf.last() == Some(&b'\n') || buf.is_empty() {
            lines.pop();
        }
        'line: for l in lines {
            if l.is_empty() {
                continue;
            }
            let mut row = Vec::new();
            for f in l.split(|&c| c == b',') {
                if f.contains(&b'"') {
                    if f.len() >= 2 && f[0] == b'"' && f[f.len() - 1] == b'"' && !f[1..f.len() - 1].contains(&b'"') {
                        row.push(f[1..f.len() - 1].to_vec());
                    } else {
                        continue 'line;
                    }
                } else {
                    row.push(f.to_vec());
                }
            }
            if row.len() == N {
                out.push(row);
            }
        }
        out
    }

    #[test]
    fn fuzz_lines() {
        let alphabet = b"ab,\"\n\r x";
        let mut state = 0x9E3779B97F4A7C15u64;
        let mut rnd = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let sparse = b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab\"\",\n";
        for it in 0..40000 {
            let alpha: &[u8] = if it % 2 == 0 { alphabet } else { sparse };
            let n = (rnd() % 600) as usize;
            let buf: Vec<u8> = (0..n).map(|_| alpha[(rnd() % alpha.len() as u64) as usize]).collect();
            let mut got = Vec::new();
            for_each_line::<3, _>(&buf, |f| got.push(f.iter().map(|x| x.to_vec()).collect::<Vec<_>>()));
            assert_eq!(got, naive::<3>(&buf), "{:?}", String::from_utf8_lossy(&buf));
        }
    }
}
