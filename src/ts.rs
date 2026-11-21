//! Timestamp and price parsing, ASCII digits only.


pub const DAY: i64 = 86_400_000;

pub fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let yoe = year - era * 400;
    let doy = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

#[inline]
fn is_leap(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

const Y0: i64 = 1900;
const YEARS: usize = 300;

const fn civil_days(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let yoe = year - era * 400;
    let doy = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

const fn leap(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

/// For each month of the years 1900 to 2199: days since the epoch of its
/// first day, and its length. Built at compile time.
static MONTH_FIRST: [i32; YEARS * 12] = {
    let mut t = [0i32; YEARS * 12];
    let mut i = 0;
    while i < YEARS * 12 {
        t[i] = civil_days(Y0 + (i / 12) as i64, (i % 12) as i64 + 1, 1) as i32;
        i += 1;
    }
    t
};
static MONTH_LEN: [u8; YEARS * 12] = {
    let mut t = [0u8; YEARS * 12];
    let mut i = 0;
    while i < YEARS * 12 {
        let m = i % 12 + 1;
        t[i] = match m {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            _ => {
                if leap(Y0 + (i / 12) as i64) { 29 } else { 28 }
            }
        };
        i += 1;
    }
    t
};

/// Kept for callers that initialise explicitly; the tables are static now.
pub fn init() {}

#[inline]
fn slow_days(year: i64, month: i64, day: i64) -> Option<i64> {
    let dim = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if is_leap(year) { 29 } else { 28 }
        }
    };
    if day > dim { None } else { Some(days_from_civil(year, month, day)) }
}

/// `YYYY-MM-DDTHH:MM:SS` at the start of `s` (at least 20 bytes), as epoch
/// ms in local time. Validates and converts eight bytes at a time.
#[inline(always)]
fn civil_ms(s: &[u8]) -> Option<i64> {
    const Z: u64 = 0x3030_3030_3030_3030;
    const ADD: u64 = 0x7676_7676_7676_7676;
    let a = u64::from_le_bytes(s[0..8].try_into().unwrap()); // YYYY-MM-
    let b = u64::from_le_bytes(s[8..16].try_into().unwrap()); // DDTHH:MM
    let c = u32::from_le_bytes(s[16..20].try_into().unwrap()); // :SS?
    // Separators.
    let sep_ok = (a & 0xFF00_00FF_0000_0000) == 0x2D00_002D_0000_0000
        && (b & 0x0000_FF00_00FF_0000) == 0x0000_3A00_0054_0000
        && (c & 0xFF) == 0x3A;
    // Digits: after xor with '0' every digit byte must be 0..=9.
    let da = (a ^ Z) & 0x00FF_FF00_FFFF_FFFF;
    let db = (b ^ Z) & 0xFFFF_00FF_FF00_FFFF;
    let dc = ((c ^ 0x3030_3030) & 0x00FF_FF00) as u64;
    let bad = ((da.wrapping_add(ADD) | da) & 0x0080_8000_8080_8080)
        | ((db.wrapping_add(ADD) | db) & 0x8080_0080_8000_8080)
        | ((dc.wrapping_add(ADD) | dc) & 0x0000_0000_0080_8000);
    if !sep_ok || bad != 0 {
        return None;
    }
    // Pairs of digits: byte i becomes 10 * d[i] + d[i + 1].
    let pa = da.wrapping_mul(10).wrapping_add(da >> 8);
    let pb = db.wrapping_mul(10).wrapping_add(db >> 8);
    let year = (pa & 0xFF) as i64 * 100 + ((pa >> 16) & 0xFF) as i64;
    let month = ((pa >> 40) & 0xFF) as i64;
    let day = (pb & 0xFF) as i64;
    let hour = ((pb >> 24) & 0xFF) as i64;
    let minute = ((pb >> 48) & 0xFF) as i64;
    let second = ((dc >> 8) & 0xFF) as i64 * 10 + ((dc >> 16) & 0xFF) as i64;
    if !(1..=12).contains(&month) || day < 1 || hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let days = if (Y0..Y0 + YEARS as i64).contains(&year) {
        let idx = ((year - Y0) * 12 + month - 1) as usize;
        // idx < YEARS * 12 by the range checks above.
        if day > (unsafe { *MONTH_LEN.get_unchecked(idx) }) as i64 {
            return None;
        }
        (unsafe { *MONTH_FIRST.get_unchecked(idx) }) as i64 + day - 1
    } else {
        slow_days(year, month, day)?
    };
    Some((days * 86_400 + (hour * 60 + minute) * 60 + second) * 1000)
}


/// Parses F1, F2 or F3 into epoch milliseconds. None for anything else,
/// including an F3 value too large for i64 (it is outside any window).
#[inline(always)]
pub fn parse_ts(s: &[u8]) -> Option<i64> {
    let n = s.len();
    if n == 20 {
        if s[19] != b'Z' {
            return parse_digits(s);
        }
        return civil_ms(s);
    }
    if n == 25 && (s[19] == b'+' || s[19] == b'-') {
        if s[22] != b':' {
            return None;
        }
        let base = civil_ms(s)?;
        let (a, b, c, d) = (s[20].wrapping_sub(b'0'), s[21].wrapping_sub(b'0'), s[23].wrapping_sub(b'0'), s[24].wrapping_sub(b'0'));
        if a > 9 || b > 9 || c > 9 || d > 9 {
            return None;
        }
        let oh = (a * 10 + b) as i64;
        let om = (c * 10 + d) as i64;
        if oh > 23 || om > 59 {
            return None;
        }
        let off = (oh * 60 + om) * 60_000;
        return Some(if s[19] == b'+' { base - off } else { base + off });
    }
    parse_digits(s)
}

/// F3: ASCII digits only, leading zeros allowed.
#[inline(always)]
fn parse_digits(s: &[u8]) -> Option<i64> {
    if s.is_empty() {
        return None;
    }
    if s.len() <= 18 {
        let mut v: i64 = 0;
        for &c in s {
            let d = c.wrapping_sub(b'0');
            if d > 9 {
                return None;
            }
            v = v * 10 + d as i64;
        }
        return Some(v);
    }
    parse_digits_long(s)
}

#[cold]
fn parse_digits_long(s: &[u8]) -> Option<i64> {
    let mut v: i64 = 0;
    for &c in s {
        if !c.is_ascii_digit() {
            return None;
        }
        v = v.checked_mul(10)?.checked_add((c - b'0') as i64)?;
    }
    Some(v)
}

/// A purchase price: digits only, 1 to 10,000,000.
#[inline(always)]
pub fn parse_price(s: &[u8]) -> Option<u32> {
    if s.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &b in s {
        let d = b.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = v * 10 + d as u64;
        if v > 10_000_000 {
            return None;
        }
    }
    if v == 0 { None } else { Some(v as u32) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forms() {
        init();
        let f1 = parse_ts(b"2026-10-03T23:58:10Z").unwrap();
        assert_eq!(f1, 1791071890000);
        assert_eq!(parse_ts(b"2026-10-03T19:58:10-04:00"), Some(f1));
        assert_eq!(parse_ts(b"1791071890000"), Some(f1));
        assert_eq!(parse_ts(b"0001791071890000"), Some(f1));
        assert_eq!(parse_ts(b"00000000000000000001791071890000"), Some(f1));
        assert_eq!(parse_ts(b"2026-10-04T01:30:00+02:00"), parse_ts(b"2026-10-03T23:30:00Z"));
        assert_eq!(parse_ts(b"2026-10-03T10:00:00-00:00"), parse_ts(b"2026-10-03T10:00:00Z"));
        for bad in [
            &b""[..], b"2026-10-03T23:58:10z", b"2026-10-03t23:58:10Z", b"2026-10-03 23:58:10Z",
            b"2026-10-03T23:58:10", b"2026-10-03T23:58:10.000Z", b"1791071890000.0", b"1.79e12", b"-5",
            b"2026-13-03T23:58:10Z", b"2026-02-30T23:58:10Z", b"2027-02-29T23:58:10Z", b"2026-10-03T24:00:00Z",
            b"2026-10-03T23:58:60Z", b"2026-10-03T23:58:10+24:00", b"2026-10-03T23:58:10+05:60",
            b"2026-10-03T23:58:10+0200", b"99999999999999999999", "\u{0662}\u{0665}".as_bytes(),
        ] {
            assert_eq!(parse_ts(bad), None, "{:?}", String::from_utf8_lossy(bad));
        }
        assert!(parse_ts(b"2028-02-29T00:00:00Z").is_some());
        assert!(parse_ts(b"2000-02-29T00:00:00Z").is_some());
        assert!(parse_ts(b"2100-02-29T00:00:00Z").is_none());
        assert_eq!(parse_ts(b"86400000"), Some(86_400_000));
        assert_eq!(parse_price(b"0250"), Some(250));
        assert_eq!(parse_price(b"0"), None);
        assert_eq!(parse_price(b"10000000"), Some(10_000_000));
        assert_eq!(parse_price(b"10000001"), None);
        assert_eq!(parse_price(b"19.99"), None);
        assert_eq!(parse_price(b"1000\r"), None);
    }
}

#[cfg(test)]
mod fuzz {
    use super::*;

    /// The SWAR parser agrees with a plain reference on random F1 strings,
    /// valid and broken.
    #[test]
    fn f1_matches_reference() {
        init();
        fn reference(s: &[u8]) -> Option<i64> {
            if s.len() != 20 || s[19] != b'Z' || s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
                return None;
            }
            let num = |a: usize, n: usize| -> Option<i64> {
                let mut v = 0i64;
                for &c in &s[a..a + n] {
                    if !c.is_ascii_digit() {
                        return None;
                    }
                    v = v * 10 + (c - b'0') as i64;
                }
                Some(v)
            };
            let (y, mo, d, h, mi, se) = (num(0, 4)?, num(5, 2)?, num(8, 2)?, num(11, 2)?, num(14, 2)?, num(17, 2)?);
            if !(1..=12).contains(&mo) || d < 1 || h > 23 || mi > 59 || se > 59 {
                return None;
            }
            let dim = [31, if is_leap(y) { 29 } else { 28 }, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][mo as usize - 1];
            if d > dim {
                return None;
            }
            Some(days_from_civil(y, mo, d) * DAY + ((h * 60 + mi) * 60 + se) * 1000)
        }
        let mut x: u64 = 0x1234_5678_9abc_def1;
        let mut rnd = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        let alphabet = b"0123456789-T:Z /\\x7fA\\x80";
        for _ in 0..2_000_000 {
            let mut s = *b"2028-02-29T23:59:59Z";
            let r = rnd();
            // Mostly valid-looking digits, with random corruption.
            for i in 0..19 {
                if s[i].is_ascii_digit() {
                    s[i] = b'0' + (rnd() % 10) as u8;
                }
            }
            if r % 3 == 0 {
                let i = (rnd() % 20) as usize;
                s[i] = alphabet[(rnd() % alphabet.len() as u64) as usize];
            }
            if r % 5 == 0 {
                s[0] = b'2';
                s[1] = b'0';
                s[2] = b'2';
                s[5] = b'0' + (rnd() % 2) as u8;
                s[8] = b'0' + (rnd() % 4) as u8;
            }
            assert_eq!(parse_ts(&s), reference(&s), "{}", String::from_utf8_lossy(&s));
        }
    }
}

/// `parse_ts` for a field followed by at least 16 readable bytes: digit-only
/// values (F3) up to 16 digits are converted with SIMD.
#[inline(always)]
pub fn parse_ts_fast(s: &[u8], readable16: bool) -> Option<i64> {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        let n = s.len();
        if readable16 && (1..=16).contains(&n) && n != 20 {
            if let Some(v) = digits16(s) {
                return Some(v);
            }
            // Not all digits: one of the other forms, or bad.
            if n < 20 {
                return None;
            }
        }
    }
    parse_ts(s)
}

/// Up to 16 ASCII digits at `s` (16 bytes readable) as a number.
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
fn digits16(s: &[u8]) -> Option<i64> {
    use std::arch::x86_64::*;
    static SHIFT: [[i8; 16]; 17] = {
        // For length L, lane j takes input byte j - (16 - L), or zero.
        let mut t = [[-128i8; 16]; 17];
        let mut l = 0;
        while l <= 16 {
            let mut j = 16 - l;
            while j < 16 {
                t[l][j] = (j - (16 - l)) as i8;
                j += 1;
            }
            l += 1;
        }
        t
    };
    let n = s.len();
    unsafe {
        let raw = _mm_loadu_si128(s.as_ptr() as *const __m128i);
        let d = _mm_sub_epi8(raw, _mm_set1_epi8(b'0' as i8));
        // Lanes past the end are ignored; every live lane must be 0..=9.
        let idx = _mm_setr_epi8(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
        let live = _mm_cmpgt_epi8(_mm_set1_epi8(n as i8), idx);
        let over = _mm_cmpgt_epi8(_mm_max_epu8(d, _mm_set1_epi8(9)), _mm_set1_epi8(9));
        let bad = _mm_or_si128(_mm_and_si128(over, live), _mm_and_si128(_mm_cmplt_epi8(d, _mm_setzero_si128()), live));
        if _mm_movemask_epi8(bad) != 0 {
            return None;
        }
        // Right-align the digits (leading zeros), then combine pairwise.
        let x = _mm_shuffle_epi8(d, _mm_loadu_si128(SHIFT[n].as_ptr() as *const __m128i));
        let p2 = _mm_maddubs_epi16(x, _mm_setr_epi8(10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1));
        let p4 = _mm_madd_epi16(p2, _mm_setr_epi16(100, 1, 100, 1, 100, 1, 100, 1));
        let p4 = _mm_packus_epi32(p4, p4);
        let p8 = _mm_madd_epi16(p4, _mm_setr_epi16(10000, 1, 10000, 1, 10000, 1, 10000, 1));
        let hi = _mm_cvtsi128_si32(p8) as u32 as i64;
        let lo = _mm_extract_epi32(p8, 1) as u32 as i64;
        Some(hi * 100_000_000 + lo)
    }
}

#[cfg(test)]
mod fast_tests {
    use super::*;

    #[test]
    fn digits_match() {
        init();
        let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
        for _ in 0..500_000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let len = (x % 18) as usize + 1;
            let mut s: Vec<u8> = (0..len).map(|i| b'0' + ((x >> (i * 3)) % 10) as u8).collect();
            if x % 7 == 0 {
                let i = (x >> 40) as usize % len;
                s[i] = [b'.', b'e', b'-', b'/', b':', 0xd9, b' '][(x >> 50) as usize % 7];
            }
            let mut padded = s.clone();
            padded.extend_from_slice(&[b'7'; 16]);
            assert_eq!(parse_ts_fast(&padded[..len], true), parse_ts(&s), "{:?}", String::from_utf8_lossy(&s));
        }
        for t in [&b"2026-10-03T23:58:10Z"[..], b"2026-10-03T19:58:10-04:00", b"1791071890000", b"0001791071890000", b""] {
            let mut p = t.to_vec();
            p.extend_from_slice(&[b'1'; 16]);
            assert_eq!(parse_ts_fast(&p[..t.len()], true), parse_ts(t));
        }
    }
}
