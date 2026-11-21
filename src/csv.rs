//! The CSV dialect of rules.md: only `\n` ends a line, a `\r` is data, quotes
//! may wrap a whole field and are syntax, and a value never holds a comma, a
//! quote or a newline.
//!
//! `scan` walks a chunk 64 bytes at a time, finding commas, quotes and line
//! breaks with SIMD compares, and hands each well-formed line to a callback as
//! its N unquoted fields. Malformed lines (wrong field count, a quote anywhere
//! but around a whole field, empty lines) never reach the callback.

#[inline(always)]
fn finish(f: &[u8], quotes: u32) -> Option<&[u8]> {
    if quotes == 0 {
        Some(f)
    } else if quotes == 2 && f.len() >= 2 && f[0] == b'"' && f[f.len() - 1] == b'"' {
        Some(&f[1..f.len() - 1])
    } else {
        None
    }
}

/// Bit masks of `,` and `"` and `\n` in a 64-byte block.
#[inline(always)]
fn masks(block: &[u8; 64]) -> (u64, u64, u64) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    unsafe {
        use std::arch::x86_64::*;
        let lo = _mm256_loadu_si256(block.as_ptr() as *const __m256i);
        let hi = _mm256_loadu_si256(block.as_ptr().add(32) as *const __m256i);
        let m = |v: __m256i, c: i8| -> u32 { _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, _mm256_set1_epi8(c))) as u32 };
        let comma = m(lo, b',' as i8) as u64 | (m(hi, b',' as i8) as u64) << 32;
        let quote = m(lo, b'"' as i8) as u64 | (m(hi, b'"' as i8) as u64) << 32;
        let nl = m(lo, b'\n' as i8) as u64 | (m(hi, b'\n' as i8) as u64) << 32;
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

/// Calls `f` with the N fields of every well-formed line in `chunk`.
#[inline(always)]
pub fn scan<'a, const N: usize, F: FnMut(&[&'a [u8]; N])>(chunk: &'a [u8], mut f: F) {
    let n = chunk.len();
    let mut fields: [&'a [u8]; N] = [&[]; N];
    let mut nf = 0usize; // fields completed in the current line
    let mut fstart = 0usize; // start of the current field
    let mut quotes = 0u32;
    let mut bad = false;
    let mut line_start = 0usize;
    let mut base = 0usize;
    let mut tail: [u8; 64];
    while base < n {
        let block: &[u8; 64] = if base + 64 <= n {
            chunk[base..base + 64].try_into().unwrap()
        } else {
            tail = [0u8; 64];
            tail[..n - base].copy_from_slice(&chunk[base..]);
            &tail
        };
        let (comma, quote, nl) = masks(block);
        let mut bits = comma | quote | nl;
        while bits != 0 {
            let i = bits.trailing_zeros() as usize;
            let bit = 1u64 << i;
            bits &= bits - 1;
            let p = base + i;
            if quote & bit != 0 {
                quotes += 1;
            } else if comma & bit != 0 {
                if nf + 1 >= N {
                    bad = true;
                } else if !bad {
                    match finish(&chunk[fstart..p], quotes) {
                        Some(v) => fields[nf] = v,
                        None => bad = true,
                    }
                }
                nf += 1;
                fstart = p + 1;
                quotes = 0;
            } else {
                if p > line_start && !bad && nf + 1 == N {
                    if let Some(v) = finish(&chunk[fstart..p], quotes) {
                        fields[nf] = v;
                        f(&fields);
                    }
                }
                nf = 0;
                fstart = p + 1;
                line_start = p + 1;
                quotes = 0;
                bad = false;
            }
        }
        base += 64;
    }
    // A last line without a trailing newline.
    if n > line_start && !bad && nf + 1 == N {
        if let Some(v) = finish(&chunk[fstart..n], quotes) {
            fields[nf] = v;
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
        let r = all::<2>(b"a,b\n\"x\",\"\"\nu1,u2,u3\nu1,u5\"\n\n\"u4\",u1\r\na\nlast,one");
        assert_eq!(
            r,
            vec![
                vec![b"a".to_vec(), b"b".to_vec()],
                vec![b"x".to_vec(), b"".to_vec()],
                vec![b"u4".to_vec(), b"u1\r".to_vec()],
                vec![b"last".to_vec(), b"one".to_vec()],
            ]
        );
        let long: Vec<u8> = (0..300).flat_map(|i| format!("id{i},v{i}\n").into_bytes()).collect();
        assert_eq!(all::<2>(&long).len(), 300);
    }
}
