//! Timestamp and price parsing, ASCII digits only.

use std::sync::OnceLock;

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

/// Days since the epoch of the first of each month, and the month's length,
/// for the years 1900 to 2199.
struct Months {
    first: Vec<i32>,
    len: Vec<u8>,
}

fn months() -> &'static Months {
    static M: OnceLock<Months> = OnceLock::new();
    M.get_or_init(|| {
        let mut first = Vec::with_capacity(YEARS * 12);
        let mut len = Vec::with_capacity(YEARS * 12);
        for y in 0..YEARS as i64 {
            for m in 1..=12 {
                first.push(days_from_civil(Y0 + y, m, 1) as i32);
                len.push(match m {
                    1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
                    4 | 6 | 9 | 11 => 30,
                    _ => {
                        if is_leap(Y0 + y) { 29 } else { 28 }
                    }
                });
            }
        }
        Months { first, len }
    })
}

/// Must be called once before parsing on many threads.
pub fn init() {
    let _ = months();
}

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

/// `YYYY-MM-DDTHH:MM:SS` at the start of `s` (at least 19 bytes), as epoch
/// ms in local time.
#[inline(always)]
fn civil_ms(s: &[u8]) -> Option<i64> {
    let b: &[u8; 19] = s[..19].try_into().unwrap();
    let sep = (b[4] ^ b'-') | (b[7] ^ b'-') | (b[10] ^ b'T') | (b[13] ^ b':') | (b[16] ^ b':');
    let v = |i: usize| b[i].wrapping_sub(b'0');
    let digits = [v(0), v(1), v(2), v(3), v(5), v(6), v(8), v(9), v(11), v(12), v(14), v(15), v(17), v(18)];
    let mut over = 0u8;
    for d in digits {
        over |= (d > 9) as u8;
    }
    if (sep | over) != 0 {
        return None;
    }
    let [y0, y1, y2, y3, m0, m1, d0, d1, h0, h1, i0, i1, s0, s1] = digits.map(|d| d as i64);
    let year = y0 * 1000 + y1 * 100 + y2 * 10 + y3;
    let month = m0 * 10 + m1;
    let day = d0 * 10 + d1;
    let hour = h0 * 10 + h1;
    let minute = i0 * 10 + i1;
    let second = s0 * 10 + s1;
    if !(1..=12).contains(&month) || day < 1 || hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let days = if (Y0..Y0 + YEARS as i64).contains(&year) {
        let mt = months();
        let idx = ((year - Y0) * 12 + month - 1) as usize;
        if day > mt.len[idx] as i64 {
            return None;
        }
        mt.first[idx] as i64 + day - 1
    } else {
        slow_days(year, month, day)?
    };
    Some(days * DAY + ((hour * 60 + minute) * 60 + second) * 1000)
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
