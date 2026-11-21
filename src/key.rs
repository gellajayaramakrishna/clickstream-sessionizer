//! IDs packed into a u64 so they compare and hash as integers.
//!
//! An ID of up to 10 characters from the 64-symbol alphabet
//! ` 0-9 A-Z _ a-z` is packed as 6-bit codes, first character highest, with
//! its length in the lowest 4 bits. The codes follow byte order, so comparing
//! two packed keys as integers is exactly the byte-wise comparison of the IDs
//! (the length breaks prefix ties).
//!
//! Any other ID is interned in a global table and marked with 15 in the low
//! 4 bits. Such keys are equal only to themselves, and `key_cmp` resolves
//! them to bytes whenever order matters.

use std::cmp::Ordering;
use std::hash::{BuildHasher, Hasher};
use std::sync::{Mutex, OnceLock};

pub type Key = u64;

const LONG: u64 = 15;
const MAXLEN: usize = 10;

const fn build_codes() -> [u8; 256] {
    let mut t = [0xFFu8; 256];
    let mut next = 0u8;
    let mut b = 0usize;
    while b < 256 {
        let c = b as u8;
        if c == b' ' || c.is_ascii_digit() || c.is_ascii_uppercase() || c == b'_' || c.is_ascii_lowercase() {
            t[b] = next;
            next += 1;
        }
        b += 1;
    }
    t
}

const fn build_chars() -> [u8; 64] {
    let codes = build_codes();
    let mut t = [0u8; 64];
    let mut b = 0usize;
    while b < 256 {
        if codes[b] != 0xFF {
            t[codes[b] as usize] = b as u8;
        }
        b += 1;
    }
    t
}

static CODES: [u8; 256] = build_codes();
static CHARS: [u8; 64] = build_chars();

struct LongTable {
    index: std::collections::HashMap<Vec<u8>, u64>,
    strings: Vec<Vec<u8>>,
}

fn long_table() -> &'static Mutex<LongTable> {
    static T: OnceLock<Mutex<LongTable>> = OnceLock::new();
    T.get_or_init(|| Mutex::new(LongTable { index: Default::default(), strings: Vec::new() }))
}

#[inline(always)]
pub fn pack(b: &[u8]) -> Key {
    let n = b.len();
    if n > MAXLEN {
        return pack_long(b);
    }
    let mut v: u64 = 0;
    let mut bad: u8 = 0;
    for &c in b {
        let code = CODES[c as usize];
        bad |= code;
        v = (v << 6) | (code as u64 & 63);
    }
    if bad == 0xFF {
        return pack_long(b);
    }
    v <<= 6 * (MAXLEN - n) as u32;
    (v << 4) | n as u64
}

#[cold]
#[inline(never)]
fn pack_long(b: &[u8]) -> Key {
    let mut t = long_table().lock().unwrap();
    let id = match t.index.get(b) {
        Some(&i) => i,
        None => {
            let i = t.strings.len() as u64;
            t.strings.push(b.to_vec());
            t.index.insert(b.to_vec(), i);
            i
        }
    };
    (id << 4) | LONG
}

#[inline(always)]
pub fn is_long(k: Key) -> bool {
    (k & 15) == LONG
}

/// Appends the bytes of the ID behind `k` to `out`.
pub fn key_write(k: Key, out: &mut Vec<u8>) {
    if is_long(k) {
        let id = (k >> 4) as usize;
        out.extend_from_slice(&long_table().lock().unwrap().strings[id]);
    } else {
        let n = (k & 15) as usize;
        let v = k >> 4;
        for i in 0..n {
            let code = (v >> (6 * (MAXLEN - 1 - i))) & 63;
            out.push(CHARS[code as usize]);
        }
    }
}

pub fn key_bytes(k: Key) -> Vec<u8> {
    let mut v = Vec::with_capacity(MAXLEN);
    key_write(k, &mut v);
    v
}

/// Byte-wise order of the IDs behind two keys.
#[inline(always)]
pub fn key_cmp(a: Key, b: Key) -> Ordering {
    if !is_long(a) && !is_long(b) { a.cmp(&b) } else { key_bytes(a).cmp(&key_bytes(b)) }
}

#[inline(always)]
pub fn hash_key(k: Key) -> u64 {
    let m = (k as u128).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let x = (m as u64) ^ ((m >> 64) as u64);
    x.wrapping_mul(0xD6E8_FEB8_6659_FD93) ^ (x >> 29)
}

#[derive(Clone, Copy, Default)]
pub struct KeyBuild;

pub struct KeyHasher(u64);

impl Hasher for KeyHasher {
    #[inline(always)]
    fn finish(&self) -> u64 {
        self.0
    }
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = (self.0 ^ b as u64).wrapping_mul(0x100_0000_01B3);
        }
    }
    #[inline(always)]
    fn write_u64(&mut self, k: u64) {
        self.0 = hash_key(k);
    }
}

impl BuildHasher for KeyBuild {
    type Hasher = KeyHasher;
    #[inline(always)]
    fn build_hasher(&self) -> KeyHasher {
        KeyHasher(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fast_matches_slow() {
        let mut buf = Vec::new();
        let ids: Vec<&[u8]> = vec![b"u1", b"e42huhoug", b"U8W84O3T0", b" u1", b"u1 ", b"c_1190", b"p13649", b"zzzzzzzzzz",
            b"0", b"a", b"_", b"Z", b" ", b"u\t1", b"u1\r", b"\xd9\xa2", b"abcdefghijk", b"9y"];
        for id in ids {
            buf.clear();
            buf.extend_from_slice(id);
            buf.extend_from_slice(&[b'x'; 16]);
            assert_eq!(pack_fast(&buf[..id.len()], true), pack(id), "{:?}", id);
        }
    }

    #[test]
    fn order_and_roundtrip() {
        let ids: Vec<&[u8]> = vec![
            b"", b" ", b" u1", b"0", b"A", b"U1", b"_", b"a", b"u1", b"u1 ", b"u10", b"u2", b"zzzzzzzzzz",
            b"zzzzzzzzzzz", b"u\t1", b"\xd9\xa2", b"e3\r",
        ];
        for a in &ids {
            assert_eq!(key_bytes(pack(a)), a.to_vec());
            for b in &ids {
                assert_eq!(key_cmp(pack(a), pack(b)), a.cmp(b), "{a:?} {b:?}");
                assert_eq!(pack(a) == pack(b), a == b);
            }
        }
    }
}

/// `pack` for an ID whose 16 bytes starting at `p` can be read (the ID is
/// `len` bytes; the rest is ignored). Uses SIMD to map up to 10 characters
/// to their codes at once; anything unusual goes to `pack`.
#[inline(always)]
pub fn pack_fast(b: &[u8], readable16: bool) -> Key {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        let n = b.len();
        if readable16 && n <= MAXLEN {
            unsafe {
                use std::arch::x86_64::*;
                let v = _mm_loadu_si128(b.as_ptr() as *const __m128i);
                // Lanes past the end of the ID.
                let idx = _mm_setr_epi8(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
                let live = _mm_cmpgt_epi8(_mm_set1_epi8(n as i8), idx);
                // Byte classes (all values here are below 0x80 when valid).
                let ge = |c: u8| _mm_cmpgt_epi8(v, _mm_set1_epi8(c as i8 - 1));
                let le = |c: u8| _mm_cmpgt_epi8(_mm_set1_epi8(c as i8 + 1), v);
                let digit = _mm_and_si128(ge(b'0'), le(b'9'));
                let upper = _mm_and_si128(ge(b'A'), le(b'Z'));
                let lower = _mm_and_si128(ge(b'a'), le(b'z'));
                let space = _mm_cmpeq_epi8(v, _mm_set1_epi8(b' ' as i8));
                let under = _mm_cmpeq_epi8(v, _mm_set1_epi8(b'_' as i8));
                let any = _mm_or_si128(_mm_or_si128(digit, upper), _mm_or_si128(lower, _mm_or_si128(space, under)));
                let bad = _mm_andnot_si128(any, live);
                if _mm_movemask_epi8(bad) == 0 {
                    // code = byte - offset of its class.
                    let off = _mm_or_si128(
                        _mm_or_si128(_mm_and_si128(digit, _mm_set1_epi8(47)), _mm_and_si128(upper, _mm_set1_epi8(54))),
                        _mm_or_si128(
                            _mm_and_si128(lower, _mm_set1_epi8(59)),
                            _mm_or_si128(_mm_and_si128(space, _mm_set1_epi8(32)), _mm_and_si128(under, _mm_set1_epi8(58))),
                        ),
                    );
                    let codes = _mm_and_si128(_mm_sub_epi8(v, off), live);
                    // Pairs -> 12 bits, quads -> 24 bits, first character highest.
                    let pairs = _mm_maddubs_epi16(codes, _mm_set1_epi16(0x0140));
                    let quads = _mm_madd_epi16(pairs, _mm_set1_epi32(0x0001_1000));
                    let q0 = _mm_cvtsi128_si32(quads) as u32 as u64;
                    let q1 = _mm_extract_epi32(quads, 1) as u32 as u64;
                    let q2 = _mm_extract_epi32(quads, 2) as u32 as u64;
                    let v60 = (q0 << 36) | (q1 << 12) | (q2 >> 12);
                    return (v60 << 4) | n as u64;
                }
            }
        }
    }
    let _ = readable16;
    pack(b)
}
