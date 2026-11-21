//! The CSV dialect of rules.md: only `\n` ends a line, a `\r` is data, quotes
//! may wrap a whole field and are syntax, and a value never holds a comma, a
//! quote or a newline.
//!
//! `scan` walks a chunk 64 bytes at a time, finding commas, quotes and line
//! breaks with SIMD compares. It only records where the commas are; quotes
//! are checked once per line (see `finish_line`). Each well-formed line
//! reaches the callback as its N unquoted fields. Malformed lines (wrong field
//! count, a quote anywhere but around a whole field, empty lines) never do.

/// Most fields a line may have.
const MAXF: usize = 8;

/// Bit masks of `,` and `"` and `\n` in a 64-byte block.
#[inline(always)]
fn masks(block: &[u8; 64]) -> (u64, u64, u64) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    unsafe {
        use std::arch::x86_64::*;
        let lo = _mm256_loadu_si256(block.as_ptr() as *const __m256i);
        let hi = _mm256_loadu_si256(block.as_ptr().add(32) as *const __m256i);
        let m = |v: __m256i, c: i8| -> u64 { _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, _mm256_set1_epi8(c))) as u32 as u64 };
        let comma = m(lo, b',' as i8) | m(hi, b',' as i8) << 32;
        let quote = m(lo, b'"' as i8) | m(hi, b'"' as i8) << 32;
        let nl = m(lo, b'\n' as i8) | m(hi, b'\n' as i8) << 32;
        (comma, quote, nl)
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
    {
        let (mut comma, mut quote, mut nl) = (0u64, 0u64, 0u64);
        for (i, &c) in block.iter().enumerate() {
            comma |= ((c == b',') as u64) << i;
            quote |= ((c == b'"') as u64) << i;
            nl |= ((c == b'\n') as u64) << i;
        }
        (comma, quote, nl)
    }
}

/// Builds the N fields of one line and checks its quotes. Field k runs from
/// `seps[k] + 1` to `seps[k + 1]` (`seps[0]` is the line start - 1).
///
/// A line's quotes are well formed exactly when every field that starts with
/// a quote is at least 2 long and ends with one, and the line holds exactly
/// two quotes per such field: then no quote can be anywhere else.
#[inline(always)]
fn finish_line<'a, const N: usize>(ptr: *const u8, seps: &[usize; MAXF + 1], quotes: u32, fields: &mut [&'a [u8]; N]) -> bool {
    let mut starts = 0u32;
    let mut ok = true;
    for k in 0..N {
        let s = seps[k].wrapping_add(1);
        let e = seps[k + 1];
        let len = e - s;
        unsafe {
            let first = if len > 0 { *ptr.add(s) } else { 0 };
            let last = if len > 0 { *ptr.add(e - 1) } else { 0 };
            let q = first == b'"';
            ok &= !q | ((len >= 2) & (last == b'"'));
            starts += q as u32;
            let st = q as usize;
            fields[k] = std::slice::from_raw_parts(ptr.add(s + st), len.saturating_sub(2 * st));
        }
    }
    ok & (quotes == 2 * starts)
}

/// Calls `f` with the N fields of every well-formed line in `chunk`.
#[inline(always)]
pub fn scan<'a, const N: usize, F: FnMut(&[&'a [u8]; N])>(chunk: &'a [u8], mut f: F) {
    debug_assert!(N >= 1 && N <= MAXF);
    let n = chunk.len();
    let ptr = chunk.as_ptr();
    let mut fields: [&'a [u8]; N] = [&[]; N];
    let mut seps = [usize::MAX; MAXF + 1];
    let mut commas = 0usize;
    let mut q_line = 0u32; // quotes before the line start
    let mut q_block = 0u32; // quotes before the current block
    let mut line_start = 0usize;
    let mut base = 0usize;
    let mut tail: [u8; 64];
    while base < n {
        let block: &[u8; 64] = if base + 64 <= n {
            unsafe { &*(ptr.add(base) as *const [u8; 64]) }
        } else {
            tail = [0u8; 64];
            tail[..n - base].copy_from_slice(&chunk[base..]);
            &tail
        };
        let (comma, quote, nl) = masks(block);
        let mut bits = comma | nl;
        while bits != 0 {
            let i = bits.trailing_zeros();
            bits &= bits - 1;
            let p = base + i as usize;
            if (nl >> i) & 1 == 0 {
                commas += 1;
                if commas < N {
                    seps[commas] = p;
                }
            } else {
                let q_here = q_block + (quote & ((1u64 << i) - 1)).count_ones();
                if commas + 1 == N && p > line_start {
                    seps[N] = p;
                    if finish_line(ptr, &seps, q_here - q_line, &mut fields) {
                        f(&fields);
                    }
                }
                commas = 0;
                line_start = p + 1;
                seps[0] = p;
                q_line = q_here;
            }
        }
        q_block += quote.count_ones();
        base += 64;
    }
    // A last line without a trailing newline.
    if n > line_start && commas + 1 == N {
        seps[N] = n;
        if finish_line(ptr, &seps, q_block - q_line, &mut fields) {
            f(&fields);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all<const N: usize>(s: &[u8]) -> Vec<Vec<Vec<u8>>> {
        let mut v = Vec::new();
        scan::<N, _>(s, |f| v.push(f.iter().map(|x| x.to_vec()).collect()));
        v
    }

    #[test]
    fn dialect() {
        let r = all::<2>(b"a,b\n\"x\",\"\"\nu1,u2,u3\nu1,u5\"\n\n\"u4\",u1\r\na\n\",b\n\"\",\"\"\"\n,\nx\"\",y\n\"a\"b\",c\nlast,\"one\"");
        assert_eq!(
            r,
            vec![
                vec![b"a".to_vec(), b"b".to_vec()],
                vec![b"x".to_vec(), b"".to_vec()],
                vec![b"u4".to_vec(), b"u1\r".to_vec()],
                vec![b"".to_vec(), b"".to_vec()],
                vec![b"last".to_vec(), b"one".to_vec()],
            ]
        );
        let long: Vec<u8> = (0..300).flat_map(|i| format!("\"id{i}\",v{i}\n").into_bytes()).collect();
        assert_eq!(all::<2>(&long).len(), 300);
        assert_eq!(all::<2>(b"a,\"b"), Vec::<Vec<Vec<u8>>>::new());
        assert_eq!(all::<2>(b"a,b"), vec![vec![b"a".to_vec(), b"b".to_vec()]]);
        assert_eq!(all::<3>(b"a,b\na,b,c,d\n1,2,3\n").len(), 1);
    }
}
