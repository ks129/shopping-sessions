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
/// Works on 64-byte blocks: SIMD compares give bit masks of separators
/// (',' and '\n') and quotes; the loop visits separators only, and a field's
/// quotes are checked with a mask over a two-block window.
#[inline(always)]
pub fn for_each_line<'a, const N: usize, F: FnMut(&[&'a [u8]; N])>(buf: &'a [u8], mut f: F) {
    let mut fields: [&'a [u8]; N] = [&[]; N];
    let mut nf = 0usize;
    let mut fstart = 0usize;
    let mut lstart = 0usize;
    let mut bad = false;

    let len = buf.len();
    let mut blk = 0usize;
    let mut qprev = 0u64;
    while blk < len {
        let (mut sep, nl, qcur) = if blk + 64 <= len {
            unsafe { masks(buf.as_ptr().add(blk)) }
        } else {
            let mut tail = [0u8; 64];
            tail[..len - blk].copy_from_slice(&buf[blk..]);
            unsafe { masks(tail.as_ptr()) }
        };
        // Quote bits of [blk - 64, blk + 64).
        let window = ((qcur as u128) << 64) | qprev as u128;
        while sep != 0 {
            let off = sep.trailing_zeros() as usize;
            sep &= sep - 1;
            let pos = blk + off;
            if !bad && nf < N {
                let flen = pos - fstart;
                let q = if fstart + 64 >= blk {
                    // fstart >= blk - 64: inside the window
                    let shift = fstart + 64 - blk;
                    let bits = (window >> shift) as u64;
                    if flen >= 64 { u64::MAX } else { bits & ((1u64 << flen) - 1) }
                } else {
                    u64::MAX // field longer than the window: slow path below
                };
                if q == 0 {
                    fields[nf] = unsafe { buf.get_unchecked(fstart..pos) };
                } else if q != u64::MAX {
                    if q.count_ones() == 2 && q & 1 == 1 && (q >> (flen - 1)) & 1 == 1 {
                        fields[nf] = unsafe { buf.get_unchecked(fstart + 1..pos - 1) };
                    } else {
                        bad = true;
                    }
                } else {
                    match unquote(&buf[fstart..pos]) {
                        Some(v) => fields[nf] = v,
                        None => bad = true,
                    }
                }
            }
            nf += 1;
            fstart = pos + 1;
            if (nl >> off) & 1 == 1 {
                if !bad && nf == N && pos > lstart {
                    f(&fields);
                }
                nf = 0;
                bad = false;
                lstart = pos + 1;
            }
        }
        qprev = qcur;
        blk += 64;
    }
    if lstart < len {
        // Last line without a newline.
        if !bad && nf < N {
            match unquote(&buf[fstart..len]) {
                Some(v) => fields[nf] = v,
                None => bad = true,
            }
        }
        nf += 1;
        if !bad && nf == N {
            f(&fields);
        }
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

/// For the 64 bytes at `p`: (separator mask: ',' or '\n', newline mask, quote mask).
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
        return (c | n, n, q);
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
        return (mask(b',') | n, n, mask(b'"'));
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
        (c | n, n, q)
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
/// fine because it lies outside every window.
#[inline]
pub fn parse_ts(b: &[u8]) -> Option<i64> {
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
