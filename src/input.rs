//! Reading the CSV files in pieces.
//!
//! Each file is cut into fixed byte ranges, and a piece keeps exactly the
//! lines that start inside its range, so every line belongs to one piece.
//! Files are mapped read-only and each thread asks the kernel to map its
//! piece's pages in one call (MADV_POPULATE_READ), which avoids both a copy
//! and a page fault per page. If mapping fails, pieces are read with `pread`
//! into a reusable buffer instead.

use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

/// Bytes past a piece read up front to finish its last line.
const OVER: u64 = 64 << 10;
/// Zero bytes kept after the data, so 16-byte loads past a field are safe.
pub const PAD: usize = 16;

pub struct Input {
    pub files: Vec<(File, u64)>,
    /// The files mapped read-only; empty if mapping failed, in which case
    /// pieces are read with `pread` instead.
    pub maps: Vec<memmap2::Mmap>,
}

#[derive(Clone, Copy)]
pub struct Piece {
    pub file: u32,
    pub start: u64,
    pub end: u64,
}

pub fn list_csv(dir: &Path) -> Vec<PathBuf> {
    let mut v = Vec::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            if p.extension().map(|x| x == "csv").unwrap_or(false) && p.is_file() {
                v.push(p);
            }
        }
    }
    v.sort();
    v
}

impl Input {
    pub fn open(dir: &Path) -> Input {
        let mut files = Vec::new();
        for p in list_csv(dir) {
            if let Ok(f) = File::open(&p) {
                let len = f.metadata().map(|m| m.len()).unwrap_or(0);
                if len > 0 {
                    files.push((f, len));
                }
            }
        }
        let maps: Vec<memmap2::Mmap> = files.iter().filter_map(|(f, _)| unsafe { memmap2::Mmap::map(f).ok() }).collect();
        let maps = if maps.len() == files.len() { maps } else { Vec::new() };
        Input { files, maps }
    }

    /// Like `load`, but returns the range inside the mapped file, after asking
    /// the kernel to map that range in one call.
    pub fn view(&self, p: Piece) -> (&[u8], usize, usize) {
        let m: &[u8] = &self.maps[p.file as usize];
        let flen = m.len();
        let (start, end) = (p.start as usize, p.end as usize);
        let from = start.saturating_sub(1);
        let first = match memchr::memchr(b'\n', &m[from..]) {
            Some(i) => from + i + 1,
            None => return (m, 0, 0),
        };
        if first > end - 1 {
            return (m, 0, 0);
        }
        let stop = if end >= flen {
            flen
        } else {
            match memchr::memchr(b'\n', &m[end - 1..]) {
                Some(j) => end - 1 + j + 1,
                None => flen,
            }
        };
        unsafe {
            let a = (m.as_ptr() as usize + first) & !4095;
            let b = m.as_ptr() as usize + stop;
            libc::madvise(a as *mut libc::c_void, b - a, libc::MADV_POPULATE_READ);
        }
        (m, first, stop)
    }

    pub fn bytes(&self) -> u64 {
        self.files.iter().map(|f| f.1).sum()
    }

    pub fn pieces(&self, size: u64, out: &mut Vec<Piece>) {
        for (i, &(_, len)) in self.files.iter().enumerate() {
            let mut s = 0;
            while s < len {
                let e = (s + size).min(len);
                out.push(Piece { file: i as u32, start: s, end: e });
                s = e;
            }
        }
    }
}

fn read_range(f: &File, buf: &mut Vec<u8>, from: u64, to: u64) {
    let n = (to - from) as usize;
    buf.clear();
    buf.resize(n + PAD, 0);
    let mut done = 0;
    while done < n {
        match f.read_at(&mut buf[done..n], from + done as u64) {
            Ok(0) => break,
            Ok(k) => done += k,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => panic!("read failed: {e}"),
        }
    }
    buf.truncate(done + PAD);
    buf[done..].fill(0);
}

/// Reads piece `p` into `buf` and returns the range of `buf` holding the
/// lines that start inside it (the header line of a file is skipped). At
/// least PAD readable bytes follow the range.
pub fn load(input: &Input, p: Piece, buf: &mut Vec<u8>) -> (usize, usize) {
    let (ref f, flen) = input.files[p.file as usize];
    // Read from one byte early, to see whether a line starts at p.start.
    let from = p.start.saturating_sub(1);
    let mut to = (p.end + OVER).min(flen);
    loop {
        read_range(f, buf, from, to);
        let n = buf.len() - PAD;
        let data = &buf[..n];
        // First line start: after the header line, or after the first line
        // break at or after p.start - 1.
        let first = match memchr::memchr(b'\n', data) {
            Some(i) => i + 1,
            None => {
                if to < flen {
                    to = (to + (to - from)).min(flen);
                    continue;
                }
                return (0, 0);
            }
        };
        // Lines starting at p.end or later belong to the next piece.
        let k = (p.end - 1 - from) as usize; // index of byte p.end - 1
        if first > k {
            return (0, 0);
        }
        if p.end >= flen {
            return (first, n);
        }
        match memchr::memchr(b'\n', &data[k..]) {
            Some(j) => return (first, k + j + 1),
            None => {
                if to < flen {
                    to = (to + (to - from)).min(flen);
                    continue;
                }
                return (first, n);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// Every line is seen exactly once, whatever the piece size.
    #[test]
    fn pieces_cover_lines_once() {
        let dir = std::env::temp_dir().join(format!("input-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let mut text = b"h1,h2\n".to_vec();
        for i in 0..500 {
            text.extend_from_slice(format!("line{i},{}\n", "x".repeat(i % 37)).as_bytes());
        }
        text.extend_from_slice(b"\n\nlast,noeol");
        std::fs::File::create(dir.join("a.csv")).unwrap().write_all(&text).unwrap();
        let expected: Vec<&[u8]> = text.split(|&c| c == b'\n').skip(1).filter(|l| !l.is_empty()).collect();
        let input = Input::open(&dir);
        for size in [1u64, 2, 3, 7, 50, 64, 1000, 100_000] {
            let mut ps = Vec::new();
            input.pieces(size, &mut ps);
            let mut seen: Vec<Vec<u8>> = Vec::new();
            let mut buf = Vec::new();
            for p in ps {
                let (a, b) = load(&input, p, &mut buf);
                for l in buf[a..b].split(|&c| c == b'\n') {
                    if !l.is_empty() {
                        seen.push(l.to_vec());
                    }
                }
            }
            let exp: Vec<Vec<u8>> = expected.iter().map(|l| l.to_vec()).collect();
            assert_eq!(seen, exp, "piece size {size}");
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
