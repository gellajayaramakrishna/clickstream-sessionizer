// Shopping Sessions pipeline: multithreaded, mmap-based, single pass per phase.
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

mod alloc;
mod pq;

#[global_allocator]
static GLOBAL: alloc::HugeAlloc = alloc::HugeAlloc;

const DAY_MS: i64 = 86_400_000;
const GAP_MS: i64 = 1_800_000;
const WEEK_MS: i64 = 7 * DAY_MS;
const LATE_MS: i64 = DAY_MS;

const NB_E: usize = 512; // dedup buckets (by event_id hash)
const NB_P: usize = 2048; // person buckets

// ---------------------------------------------------------------- hashing

#[inline(always)]
fn fmix(mut k: u64) -> u64 {
    k ^= k >> 33;
    k = k.wrapping_mul(0xff51afd7ed558ccd);
    k ^= k >> 33;
    k = k.wrapping_mul(0xc4ceb9fe1a85ec53);
    k ^= k >> 33;
    k
}

#[inline(always)]
fn hash_bytes(b: &[u8]) -> u64 {
    let n = b.len();
    let mut h: u64 = 0x9E3779B97F4A7C15 ^ (n as u64).wrapping_mul(0xff51afd7ed558ccd);
    let mut i = 0;
    while i + 8 <= n {
        let w = u64::from_le_bytes(unsafe { *(b.as_ptr().add(i) as *const [u8; 8]) });
        h = (h ^ w).wrapping_mul(0x9FB21C651E98DF25).rotate_left(29);
        i += 8;
    }
    if i < n {
        let mut w: u64 = 0;
        let mut s = 0;
        while i < n {
            w |= (b[i] as u64) << s;
            s += 8;
            i += 1;
        }
        h = (h ^ w).wrapping_mul(0x9FB21C651E98DF25).rotate_left(29);
    }
    let h = fmix(h);
    if h == 0 { 1 } else { h }
}

// packed reference to bytes living in a leaked mmap / leaked buffer
#[inline(always)]
fn pack(b: &[u8]) -> u64 {
    let len = b.len().min(0xFFFF) as u64;
    (b.as_ptr() as u64) | (len << 48)
}
#[inline(always)]
fn unpack(p: u64) -> &'static [u8] {
    let ptr = (p & 0x0000_FFFF_FFFF_FFFF) as *const u8;
    let len = (p >> 48) as usize;
    unsafe { std::slice::from_raw_parts(ptr, len) }
}

// ---------------------------------------------------------------- small open-addressing map u64 -> u32
struct HMap {
    keys: Vec<u64>,
    vals: Vec<u32>,
    mask: usize,
}
impl HMap {
    fn new(n: usize) -> Self {
        let cap = (n * 2 + 16).next_power_of_two();
        HMap { keys: vec![0; cap], vals: vec![0; cap], mask: cap - 1 }
    }
    fn insert(&mut self, k: u64, v: u32) {
        let mut i = (k as usize) & self.mask;
        loop {
            if self.keys[i] == 0 {
                self.keys[i] = k;
                self.vals[i] = v;
                return;
            }
            if self.keys[i] == k {
                self.vals[i] = v;
                return;
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline(always)]
    fn get(&self, k: u64) -> Option<u32> {
        let mut i = (k as usize) & self.mask;
        loop {
            let kk = unsafe { *self.keys.get_unchecked(i) };
            if kk == k {
                return Some(unsafe { *self.vals.get_unchecked(i) });
            }
            if kk == 0 {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }
}

// ---------------------------------------------------------------- CSV line splitting
#[inline(always)]
fn split<const N: usize>(line: &[u8], out: &mut [&'static [u8]; N]) -> bool {
    let len = line.len();
    if len == 0 {
        return false;
    }
    let p = line.as_ptr();
    let mut i = 0usize;
    let mut f = 0usize;
    loop {
        if f == N {
            return false;
        }
        if i < len && unsafe { *p.add(i) } == b'"' {
            let s = i + 1;
            let mut j = s;
            while j < len && unsafe { *p.add(j) } != b'"' {
                j += 1;
            }
            if j >= len {
                return false;
            }
            out[f] = unsafe { std::slice::from_raw_parts(p.add(s), j - s) };
            f += 1;
            j += 1;
            if j == len {
                break;
            }
            if unsafe { *p.add(j) } != b',' {
                return false;
            }
            i = j + 1;
        } else {
            let s = i;
            while i < len {
                let c = unsafe { *p.add(i) };
                if c == b',' {
                    break;
                }
                if c == b'"' {
                    return false;
                }
                i += 1;
            }
            out[f] = unsafe { std::slice::from_raw_parts(p.add(s), i - s) };
            f += 1;
            if i == len {
                break;
            }
            i += 1;
        }
    }
    f == N
}


/// AVX2 splitter for a line starting at `pos` (needs pos + 128 <= d.len()).
/// Returns (well-formed, index of the line end).
#[inline(always)]
fn split_line<const N: usize>(d: &'static [u8], pos: usize, out: &mut [&'static [u8]; N]) -> (bool, usize) {
    let n = d.len();
    #[cfg(target_arch = "x86_64")]
    {
        if pos + 128 <= n {
            unsafe {
                use std::arch::x86_64::*;
                let p = d.as_ptr().add(pos);
                let vn = _mm256_set1_epi8(b'\n' as i8);
                let vc = _mm256_set1_epi8(b',' as i8);
                let vq = _mm256_set1_epi8(b'"' as i8);
                let mut nl: u128 = 0;
                let mut cm: u128 = 0;
                let mut qm: u128 = 0;
                let mut k = 0;
                while k < 4 {
                    let x = _mm256_loadu_si256(p.add(32 * k) as *const __m256i);
                    let a = _mm256_movemask_epi8(_mm256_cmpeq_epi8(x, vn)) as u32 as u128;
                    let b = _mm256_movemask_epi8(_mm256_cmpeq_epi8(x, vc)) as u32 as u128;
                    let c = _mm256_movemask_epi8(_mm256_cmpeq_epi8(x, vq)) as u32 as u128;
                    nl |= a << (32 * k);
                    cm |= b << (32 * k);
                    qm |= c << (32 * k);
                    if a != 0 {
                        break;
                    }
                    k += 1;
                }
                if nl != 0 {
                    let l = nl.trailing_zeros() as usize;
                    let end = pos + l;
                    if l == 0 {
                        return (false, end);
                    }
                    let lm: u128 = (1u128 << l) - 1;
                    let mut c = cm & lm;
                    let q = qm & lm;
                    if c.count_ones() as usize != N - 1 {
                        return (false, end);
                    }
                    let mut s = 0usize;
                    for fi in 0..N {
                        let e = if fi + 1 < N {
                            let t = c.trailing_zeros() as usize;
                            c &= c - 1;
                            t
                        } else {
                            l
                        };
                        let mut fs = s;
                        let mut fe = e;
                        if q != 0 {
                            let m: u128 = ((1u128 << e) - 1) & !((1u128 << s) - 1);
                            let fq = q & m;
                            if fq != 0 {
                                if e < s + 2 || fq != ((1u128 << s) | (1u128 << (e - 1))) {
                                    return (false, end);
                                }
                                fs = s + 1;
                                fe = e - 1;
                            }
                        }
                        *out.get_unchecked_mut(fi) = std::slice::from_raw_parts(p.add(fs), fe - fs);
                        s = e + 1;
                    }
                    return (true, end);
                }
            }
        }
    }
    let end = memchr::memchr(b'\n', &d[pos..]).map(|x| pos + x).unwrap_or(n);
    let line: &'static [u8] = &d[pos..end];
    (split::<N>(line, out), end)
}

// ---------------------------------------------------------------- timestamps
#[inline(always)]
fn d2(b: &[u8], i: usize) -> Option<i64> {
    let a = b[i].wrapping_sub(b'0');
    let c = b[i + 1].wrapping_sub(b'0');
    if a > 9 || c > 9 {
        return None;
    }
    Some((a as i64) * 10 + c as i64)
}

#[inline(always)]
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

#[inline(always)]
fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

const CUM: [[i64; 13]; 2] = [
    [0, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334],
    [0, 0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335],
];
const DIM: [[u8; 13]; 2] = [
    [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
    [0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
];

#[inline(always)]
fn parse_dt(b: &[u8]) -> Option<i64> {
    // YYYY-MM-DDTHH:MM:SS (first 19 bytes); caller guarantees len >= 19
    let g = |i: usize| unsafe { (*b.get_unchecked(i)).wrapping_sub(b'0') };
    let sep_ok = unsafe {
        (*b.get_unchecked(4) == b'-') & (*b.get_unchecked(7) == b'-') & (*b.get_unchecked(10) == b'T') & (*b.get_unchecked(13) == b':') & (*b.get_unchecked(16) == b':')
    };
    let (y0, y1, y2, y3) = (g(0), g(1), g(2), g(3));
    let (m0, m1, d0, d1) = (g(5), g(6), g(8), g(9));
    let (h0, h1, i0, i1, s0, s1) = (g(11), g(12), g(14), g(15), g(17), g(18));
    let mx = y0.max(y1).max(y2).max(y3).max(m0).max(m1).max(d0).max(d1).max(h0).max(h1).max(i0).max(i1).max(s0).max(s1);
    if !sep_ok || mx > 9 {
        return None;
    }
    let y = y0 as i64 * 1000 + y1 as i64 * 100 + y2 as i64 * 10 + y3 as i64;
    let m = (m0 * 10 + m1) as usize;
    let d = (d0 * 10 + d1) as i64;
    let hh = (h0 * 10 + h1) as i64;
    let mi = (i0 * 10 + i1) as i64;
    let ss = (s0 * 10 + s1) as i64;
    if m < 1 || m > 12 || d < 1 || hh > 23 || mi > 59 || ss > 59 {
        return None;
    }
    let lp = is_leap(y) as usize;
    if d > DIM[lp][m] as i64 {
        return None;
    }
    let y1 = y - 1;
    let days = 365 * y1 + y1.div_euclid(4) - y1.div_euclid(100) + y1.div_euclid(400) - 719162 + CUM[lp][m] + d - 1;
    Some(((days * 86400) + hh * 3600 + mi * 60 + ss) * 1000)
}

/// Returns None for a bad timestamp; Some(i64::MAX) for digits too long for i64.
#[inline(always)]
fn parse_ts(b: &[u8]) -> Option<i64> {
    let n = b.len();
    if n == 0 {
        return None;
    }
    let c0 = b[0];
    if n == 20 && b[19] == b'Z' && b[4] == b'-' {
        return parse_dt(b);
    }
    if n == 25 && b[4] == b'-' {
        let sign = b[19];
        if (sign != b'+' && sign != b'-') || b[22] != b':' {
            return None;
        }
        let base = parse_dt(b)?;
        let oh = d2(b, 20)?;
        let om = d2(b, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        let off = (oh * 60 + om) * 60_000;
        return Some(if sign == b'+' { base - off } else { base + off });
    }
    if c0.is_ascii_digit() {
        let mut v: i64 = 0;
        let mut overflow = false;
        for &c in b {
            let d = c.wrapping_sub(b'0');
            if d > 9 {
                return None;
            }
            if !overflow {
                match v.checked_mul(10).and_then(|x| x.checked_add(d as i64)) {
                    Some(x) => v = x,
                    None => overflow = true,
                }
            }
        }
        return Some(if overflow { i64::MAX } else { v });
    }
    None
}

#[inline(always)]
fn parse_price(b: &[u8]) -> Option<u32> {
    if b.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &c in b {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = v * 10 + d as u64;
        if v > 10_000_000 {
            // keep checking digits, but value is already too large
            v = 10_000_001;
        }
    }
    if v >= 1 && v <= 10_000_000 {
        Some(v as u32)
    } else {
        None
    }
}

// ---------------------------------------------------------------- records
#[derive(Clone, Copy)]
#[repr(C)]
struct ERec {
    eid: u64,
    ingest: i64,
    ts: i64,
    user: u64,
    uref: u64,
    price: u32,
    cat: u16,
    typ: u8,
    valid: u8,
}

#[derive(Clone, Copy)]
#[repr(C)]
struct CRec {
    user: u64,
    ts: i64,
    cref: u64, // click_id bytes
    camp: u32,
    _pad: u32,
}

#[derive(Clone, Copy)]
#[repr(C)]
struct PRec {
    person: u64,
    ts: i64,
    nref: u64,
    price: u32,
    cat: u16,
    typ: u8,
    _pad: u8,
}

#[derive(Clone, Copy)]
#[repr(C)]
struct PClick {
    person: u64,
    ts: i64,
    cref: u64,
    camp: u32,
    _pad: u32,
}

// ---------------------------------------------------------------- link map
#[derive(Clone, Copy)]
#[repr(C)]
struct LEntry {
    key: u64,
    nh: u64,
    nr: u64,
}
struct LinkMap {
    slots: Vec<LEntry>,
    mask: usize,
}
impl LinkMap {
    #[inline(always)]
    fn get(&self, k: u64) -> Option<(u64, u64)> {
        if self.slots.is_empty() {
            return None;
        }
        let mut i = (k as usize) & self.mask;
        loop {
            let e = unsafe { self.slots.get_unchecked(i) };
            if e.key == k {
                return Some((e.nh, e.nr));
            }
            if e.key == 0 {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline(always)]
    fn prefetch(&self, k: u64) {
        if self.slots.is_empty() {
            return;
        }
        let i = (k as usize) & self.mask;
        prefetch_ptr(unsafe { self.slots.as_ptr().add(i) } as *const u8);
    }
}

#[inline(always)]
fn prefetch_ptr(p: *const u8) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::x86_64::_mm_prefetch(p as *const i8, std::arch::x86_64::_MM_HINT_T0);
    }
    #[cfg(not(target_arch = "x86_64"))]
    let _ = p;
}

fn uf_find(parent: &mut [u32], mut x: u32) -> u32 {
    while parent[x as usize] != x {
        let p = parent[x as usize];
        let gp = parent[p as usize];
        parent[x as usize] = gp;
        x = gp;
    }
    x
}

fn build_links(files: &[&'static [u8]]) -> LinkMap {
    let total: usize = files.iter().map(|f| f.len()).sum();
    if total == 0 {
        return LinkMap { slots: Vec::new(), mask: 0 };
    }
    build_links_big(files, (total / 16 + 64) * 2)
}

// Fallback with a caller-given capacity (only used if the estimate was too small).
fn build_links_big(files: &[&'static [u8]], cap: usize) -> LinkMap {
    let cap = cap.next_power_of_two();
    let mask = cap - 1;
    let mut slots: Vec<LEntry> = vec![LEntry { key: 0, nh: 0, nr: 0 }; cap];
    let mut node_ref: Vec<u64> = Vec::new();
    let mut node_hash: Vec<u64> = Vec::new();
    let mut parent: Vec<u32> = Vec::new();
    let mut sizes: Vec<u32> = Vec::new();
    let mut f: [&'static [u8]; 2] = [&[], &[]];
    for &data in files {
        let mut pos = match memchr::memchr(b'\n', data) {
            Some(p) => p + 1,
            None => continue,
        };
        let n = data.len();
        while pos < n {
            let end = memchr::memchr(b'\n', &data[pos..]).map(|x| pos + x).unwrap_or(n);
            let line = &data[pos..end];
            pos = end + 1;
            if !split::<2>(line, &mut f) || f[0].is_empty() || f[1].is_empty() {
                continue;
            }
            if slots.len() / 2 < node_ref.len() + 4 {
                return build_links_big(files, slots.len() * 2);
            }
            let mut ids = [0u32; 2];
            for k in 0..2 {
                let b = f[k];
                let h = hash_bytes(b);
                let mut i = (h as usize) & mask;
                ids[k] = loop {
                    let e = &mut slots[i];
                    if e.key == h {
                        break e.nr as u32;
                    }
                    if e.key == 0 {
                        let id = node_ref.len() as u32;
                        e.key = h;
                        e.nr = id as u64;
                        node_ref.push(pack(b));
                        node_hash.push(h);
                        parent.push(id);
                        sizes.push(1);
                        break id;
                    }
                    i = (i + 1) & mask;
                };
            }
            let (a, b) = (ids[0], ids[1]);
            if a != b {
                let ra = uf_find(&mut parent, a);
                let rb = uf_find(&mut parent, b);
                if ra != rb {
                    let (big, small) = if sizes[ra as usize] >= sizes[rb as usize] { (ra, rb) } else { (rb, ra) };
                    parent[small as usize] = big;
                    sizes[big as usize] += sizes[small as usize];
                }
            }
        }
    }
    finish_links(slots, mask, node_ref, node_hash, parent)
}

fn finish_links(mut slots: Vec<LEntry>, mask: usize, node_ref: Vec<u64>, node_hash: Vec<u64>, mut parent: Vec<u32>) -> LinkMap {
    let n = node_ref.len();
    let mut best: Vec<u32> = vec![u32::MAX; n];
    let mut root: Vec<u32> = vec![0; n];
    for x in 0..n as u32 {
        let r = uf_find(&mut parent, x);
        root[x as usize] = r;
        let b = best[r as usize];
        if b == u32::MAX || unpack(node_ref[x as usize]) < unpack(node_ref[b as usize]) {
            best[r as usize] = x;
        }
    }
    for e in slots.iter_mut() {
        if e.key != 0 {
            let x = e.nr as usize;
            let b = best[root[x] as usize] as usize;
            e.nh = node_hash[b];
            e.nr = node_ref[b];
        }
    }
    LinkMap { slots, mask }
}

// ---------------------------------------------------------------- inputs
fn list_csv(dir: &Path) -> Vec<PathBuf> {
    let mut v = Vec::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let name = e.file_name();
            let name = name.to_string_lossy();
            if name.starts_with('.') || !name.ends_with(".csv") {
                continue;
            }
            let p = e.path();
            if p.is_file() {
                v.push(p);
            }
        }
    }
    v.sort();
    v
}

fn map_file(p: &Path) -> &'static [u8] {
    let f = File::open(p).expect("open");
    let len = f.metadata().map(|m| m.len()).unwrap_or(0);
    if len == 0 {
        return &[];
    }
    let m = unsafe { memmap2::Mmap::map(&f).expect("mmap") };
    let m: &'static memmap2::Mmap = Box::leak(Box::new(m));
    &m[..]
}

fn read_leak(p: &Path) -> &'static [u8] {
    let v = std::fs::read(p).unwrap_or_default();
    Box::leak(v.into_boxed_slice())
}

fn lines_after_header(data: &'static [u8]) -> impl Iterator<Item = &'static [u8]> {
    let start = memchr::memchr(b'\n', data).map(|p| p + 1).unwrap_or(data.len());
    let mut pos = start;
    std::iter::from_fn(move || {
        if pos >= data.len() {
            return None;
        }
        let end = memchr::memchr(b'\n', &data[pos..]).map(|x| pos + x).unwrap_or(data.len());
        let l = &data[pos..end];
        pos = end + 1;
        Some(l)
    })
}

fn manifest_value(s: &str, key: &str) -> Option<i64> {
    let k = format!("\"{}\"", key);
    let i = s.find(&k)? + k.len();
    let rest = &s[i..];
    let q1 = rest.find('"')? + 1;
    let q2 = rest[q1..].find('"')? + q1;
    parse_ts(rest[q1..q2].as_bytes())
}

#[derive(Clone, Copy)]
struct Chunk {
    kind: u8, // 0 events, 1 clicks
    data: &'static [u8],
    start: usize,
    end: usize,
}

struct Ctx {
    ws: i64,
    we: i64,
    prod_direct: Vec<u16>,
    camp_direct: Vec<u32>,
    prod: HMap,
    unknown_cat: u16,
    camp: HMap,
}

// ---------------------------------------------------------------- phase 1: parse
fn chunk_range(c: &Chunk) -> (usize, usize) {
    let d = c.data;
    let mut s = c.start;
    if s == 0 {
        s = memchr::memchr(b'\n', d).map(|p| p + 1).unwrap_or(d.len());
    } else if d[s - 1] != b'\n' {
        s = memchr::memchr(b'\n', &d[s..]).map(|p| s + p + 1).unwrap_or(d.len());
    }
    (s, c.end)
}

/// Canonical product IDs ("p" + digits, no leading zero) map through a direct array.
#[inline(always)]
fn canon_num(b: &[u8]) -> Option<usize> {
    let n = b.len();
    if n < 2 || n > 8 || b[0] != b'p' || b[1] == b'0' {
        return None;
    }
    let mut v = 0usize;
    for &c in &b[1..] {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = v * 10 + d as usize;
    }
    Some(v)
}

#[inline(always)]
fn prod_cat(ctx: &Ctx, b: &[u8]) -> u16 {
    if let Some(v) = canon_num(b) {
        if v < ctx.prod_direct.len() {
            let c = unsafe { *ctx.prod_direct.get_unchecked(v) };
            return if c == u16::MAX { ctx.unknown_cat } else { c };
        }
    }
    ctx.prod.get(hash_bytes(b)).map(|x| x as u16).unwrap_or(ctx.unknown_cat)
}

#[inline(always)]
fn canon_camp(b: &[u8]) -> Option<usize> {
    let n = b.len();
    if n < 3 || n > 9 || b[0] != b'c' || b[1] != b'_' || b[2] == b'0' {
        return None;
    }
    let mut v = 0usize;
    for &c in &b[2..] {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = v * 10 + d as usize;
    }
    Some(v)
}

#[inline(always)]
fn camp_lookup(ctx: &Ctx, b: &[u8]) -> Option<u32> {
    if let Some(v) = canon_camp(b) {
        if v < ctx.camp_direct.len() {
            let c = unsafe { *ctx.camp_direct.get_unchecked(v) };
            return if c == u32::MAX { None } else { Some(c) };
        }
    }
    ctx.camp.get(hash_bytes(b))
}

fn parse_events(c: &Chunk, ctx: &Ctx, out: &mut Vec<Vec<ERec>>) {
    let d = c.data;
    let (mut pos, lim) = chunk_range(c);
    let mut f: [&'static [u8]; 7] = [&[]; 7];
    while pos < lim {
        let (ok, end) = split_line::<7>(d, pos, &mut f);
        pos = end + 1;
        if !ok {
            continue;
        }
        let eid = f[0];
        if eid.is_empty() {
            continue;
        }
        let eh = hash_bytes(eid);
        let ingest = parse_ts(f[3]).unwrap_or(i64::MAX);
        let mut r = ERec { eid: eh, ingest, ts: 0, user: 0, uref: 0, price: 0, cat: 0, typ: 0, valid: 0 };
        'v: {
            let user = f[1];
            if user.is_empty() || f[5].is_empty() {
                break 'v;
            }
            let ts = match parse_ts(f[2]) {
                Some(t) => t,
                None => break 'v,
            };
            if ts < ctx.ws || ts >= ctx.we {
                break 'v;
            }
            let ty = f[4];
            let typ = match ty {
                b"view" => 0u8,
                b"cart" => 1u8,
                b"purchase" => 2u8,
                _ => break 'v,
            };
            let mut price = 0u32;
            if typ == 2 {
                price = match parse_price(f[6]) {
                    Some(p) => p,
                    None => break 'v,
                };
            }
            if ingest != i64::MAX && ingest - ts > LATE_MS {
                break 'v;
            }
            if ingest == i64::MAX {
                break 'v;
            }
            let cat = prod_cat(ctx, f[5]);
            r.ts = ts;
            r.user = hash_bytes(user);
            r.uref = pack(user);
            r.price = price;
            r.cat = cat;
            r.typ = typ;
            r.valid = 1;
        }
        let b = (eh >> 55) as usize & (NB_E - 1);
        unsafe { out.get_unchecked_mut(b) }.push(r);
    }
}

fn parse_clicks(c: &Chunk, ctx: &Ctx, out: &mut Vec<CRec>) {
    let d = c.data;
    let (mut pos, lim) = chunk_range(c);
    let mut f: [&'static [u8]; 4] = [&[]; 4];
    let cw = ctx.ws - WEEK_MS;
    while pos < lim {
        let (ok, end) = split_line::<4>(d, pos, &mut f);
        pos = end + 1;
        if !ok {
            continue;
        }
        if f[0].is_empty() || f[1].is_empty() {
            continue;
        }
        let ts = match parse_ts(f[3]) {
            Some(t) => t,
            None => continue,
        };
        if ts < cw || ts >= ctx.we {
            continue;
        }
        let camp = match camp_lookup(ctx, f[2]) {
            Some(c) => c,
            None => continue,
        };
        out.push(CRec { user: hash_bytes(f[1]), ts, cref: pack(f[0]), camp, _pad: 0 });
    }
}

// ---------------------------------------------------------------- phase 3 output rows
#[derive(Clone, Copy)]
pub struct URow {
    pub nref: u64,
    pub day: i32,
    pub sessions: i64,
    pub events: i64,
    pub revenue: i64,
    pub top: i32,
}

struct Acc {
    nd: usize,
    sess: Vec<i64>,
    psess: Vec<i64>,
    rev: Vec<i64>,
    assist: Vec<i64>,
}
impl Acc {
    fn new(ncamp: usize, nd: usize) -> Self {
        let n = (ncamp + 1) * nd;
        Acc { nd, sess: vec![0; n], psess: vec![0; n], rev: vec![0; n], assist: vec![0; n] }
    }
}

struct Scratch {
    catsum: Vec<u64>,
    touched: Vec<u16>,
    starts: Vec<i64>,
    sdays: Vec<i32>,
    open: Vec<(i64, i64)>, // per campaign open interval (start, end), start=i64::MIN means none
    open_list: Vec<u32>,
    day_first: Vec<usize>,
}

fn process_bucket(
    evs: &mut Vec<PRec>,
    clicks: &mut Vec<PClick>,
    _ctx: &Ctx,
    ws_day: i64,
    ncamp: usize,
    rows: &mut Vec<URow>,
    acc: &mut Acc,
    sc: &mut Scratch,
) {
    evs.sort_unstable_by_key(|r| (r.person, r.ts));
    clicks.sort_unstable_by_key(|c| (c.person, c.ts));
    let nd = acc.nd;
    let organic = ncamp;
    let mut ci = 0usize;
    let ne = evs.len();
    let mut i = 0usize;
    while i < ne {
        let person = evs[i].person;
        let mut j = i;
        while j < ne && evs[j].person == person {
            j += 1;
        }
        // clicks of this person
        while ci < clicks.len() && clicks[ci].person < person {
            ci += 1;
        }
        let c0 = ci;
        while ci < clicks.len() && clicks[ci].person == person {
            ci += 1;
        }
        let pcl = &clicks[c0..ci];
        let nref = evs[i].nref;

        sc.starts.clear();
        sc.sdays.clear();
        // sessionize
        let mut k = i;
        let mut hi = 0usize; // click pointer: first click with ts > start
        // person-day accumulation
        let mut cur_day: i64 = i64::MIN;
        let mut d_sess = 0i64;
        let mut d_events = 0i64;
        let mut d_rev = 0i64;
        while k < j {
            let start = evs[k].ts;
            let day = start.div_euclid(DAY_MS);
            if day != cur_day {
                if cur_day != i64::MIN {
                    flush_day(rows, sc, nref, cur_day, d_sess, d_events, d_rev);
                }
                cur_day = day;
                d_sess = 0;
                d_events = 0;
                d_rev = 0;
            }
            let mut prev = start;
            let mut cnt = 0i64;
            let mut rev = 0i64;
            let mut purch = false;
            while k < j {
                let e = &evs[k];
                if e.ts - prev > GAP_MS {
                    break;
                }
                prev = e.ts;
                cnt += 1;
                if e.typ == 2 {
                    purch = true;
                    rev += e.price as i64;
                    let c = e.cat as usize;
                    if sc.catsum[c] == 0 {
                        sc.touched.push(e.cat);
                    }
                    sc.catsum[c] += e.price as u64;
                }
                k += 1;
            }
            d_sess += 1;
            d_events += cnt;
            d_rev += rev;
            let di = (day - ws_day) as usize;
            // credit
            while hi < pcl.len() && pcl[hi].ts <= start {
                hi += 1;
            }
            let mut camp = organic;
            if hi > 0 {
                let last = &pcl[hi - 1];
                if start - last.ts <= WEEK_MS {
                    let mut best = hi - 1;
                    let mut q = hi - 1;
                    while q > 0 && pcl[q - 1].ts == last.ts {
                        q -= 1;
                        if unpack(pcl[q].cref) < unpack(pcl[best].cref) {
                            best = q;
                        }
                    }
                    camp = pcl[best].camp as usize;
                }
            }
            let slot = camp * nd + di;
            acc.sess[slot] += 1;
            if purch {
                acc.psess[slot] += 1;
            }
            acc.rev[slot] += rev;
            if !pcl.is_empty() {
                sc.starts.push(start);
                sc.sdays.push(di as i32);
            }
        }
        if cur_day != i64::MIN {
            flush_day(rows, sc, nref, cur_day, d_sess, d_events, d_rev);
        }
        // assists: union of [t, t+7d] per campaign, count sessions inside per day
        if !pcl.is_empty() {
            let ns = sc.starts.len();
            // day_first[d] = first session index with day >= d
            sc.day_first.clear();
            {
                let mut s = 0usize;
                for d in 0..=nd {
                    while s < ns && (sc.sdays[s] as usize) < d {
                        s += 1;
                    }
                    sc.day_first.push(s);
                }
            }
            sc.open_list.clear();
            for c in pcl {
                let cc = c.camp as usize;
                let (os, oe) = sc.open[cc];
                if os == i64::MIN {
                    sc.open[cc] = (c.ts, c.ts + WEEK_MS);
                    sc.open_list.push(c.camp);
                } else if c.ts <= oe {
                    sc.open[cc].1 = c.ts + WEEK_MS;
                } else {
                    count_assist(acc, sc, cc, os, oe);
                    sc.open[cc] = (c.ts, c.ts + WEEK_MS);
                }
            }
            for idx in 0..sc.open_list.len() {
                let cc = sc.open_list[idx] as usize;
                let (os, oe) = sc.open[cc];
                count_assist(acc, sc, cc, os, oe);
                sc.open[cc] = (i64::MIN, i64::MIN);
            }
        }
        i = j;
    }
}

#[inline]
fn count_assist(acc: &mut Acc, sc: &Scratch, cc: usize, a: i64, b: i64) {
    let st = &sc.starts;
    let lo = st.partition_point(|&s| s < a);
    let hi = st.partition_point(|&s| s <= b);
    if lo >= hi {
        return;
    }
    let nd = acc.nd;
    let d0 = sc.sdays[lo] as usize;
    let d1 = sc.sdays[hi - 1] as usize;
    for d in d0..=d1 {
        let s = sc.day_first[d].max(lo);
        let e = sc.day_first[d + 1].min(hi);
        if e > s {
            acc.assist[cc * nd + d] += (e - s) as i64;
        }
    }
}

#[inline]
fn flush_day(rows: &mut Vec<URow>, sc: &mut Scratch, nref: u64, day: i64, s: i64, e: i64, r: i64) {
    let mut top: i32 = -1;
    let mut best: u64 = 0;
    for &c in sc.touched.iter() {
        let v = sc.catsum[c as usize];
        if v > best || (v == best && (c as i32) < top) {
            best = v;
            top = c as i32;
        }
    }
    for &c in sc.touched.iter() {
        sc.catsum[c as usize] = 0;
    }
    sc.touched.clear();
    rows.push(URow { nref, day: day as i32, sessions: s, events: e, revenue: r, top });
}

// ---------------------------------------------------------------- main
struct SendPtr<T>(*mut T);
unsafe impl<T> Send for SendPtr<T> {}
unsafe impl<T> Sync for SendPtr<T> {}

fn main() {
    let t0 = std::time::Instant::now();
    let timing = std::env::var("SS_TIMING").is_ok();
    let tick = |name: &str| if timing { eprintln!("{:>8.1} ms  {}", t0.elapsed().as_secs_f64() * 1e3, name) };
    let data_dir = PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()));
    let out_dir = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into()));
    let nthreads: usize = std::env::var("NCPU")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8))
        .max(1);

    let manifest = std::fs::read_to_string(data_dir.join("manifest.json")).expect("manifest");
    let ws = manifest_value(&manifest, "window_start").expect("window_start");
    let we = manifest_value(&manifest, "window_end").expect("window_end");
    let ws_day = ws.div_euclid(DAY_MS);
    let nd = ((we - 1).div_euclid(DAY_MS) - ws_day + 1).max(1) as usize;

    // products / categories
    let pdata = read_leak(&data_dir.join("products.csv"));
    let mut prows: Vec<(u64, &'static [u8])> = Vec::new();
    let mut f2: [&'static [u8]; 2] = [&[], &[]];
    for l in lines_after_header(pdata) {
        if split::<2>(l, &mut f2) {
            prows.push((hash_bytes(f2[0]), f2[1]));
        }
    }
    let mut cats: Vec<&'static [u8]> = prows.iter().map(|r| r.1).collect();
    cats.push(b"unknown");
    cats.sort_unstable();
    cats.dedup();
    let cat_idx = |c: &[u8]| cats.binary_search(&c).unwrap() as u32;
    let mut prod = HMap::new(prows.len());
    for &(h, c) in &prows {
        prod.insert(h, cat_idx(c));
    }
    let unknown_cat = cat_idx(b"unknown") as u16;
    // direct table for canonical ids; any canonical id beyond the table stays in the hash map
    let mut maxn = 0usize;
    let mut canon: Vec<(usize, u16)> = Vec::new();
    for l in lines_after_header(pdata) {
        if split::<2>(l, &mut f2) {
            if let Some(v) = canon_num(f2[0]) {
                if v < (1 << 22) {
                    canon.push((v, cat_idx(f2[1]) as u16));
                    maxn = maxn.max(v + 1);
                }
            }
        }
    }
    let mut prod_direct = vec![u16::MAX; maxn];
    for (v, c) in canon {
        prod_direct[v] = c;
    }

    // campaigns
    let cdata = read_leak(&data_dir.join("campaigns.csv"));
    let mut camp_ids: Vec<&'static [u8]> = Vec::new();
    let mut camp_ch: Vec<&'static [u8]> = Vec::new();
    let mut camp = HMap::new(1024);
    {
        let mut tmp: Vec<(&'static [u8], &'static [u8])> = Vec::new();
        for l in lines_after_header(cdata) {
            if split::<2>(l, &mut f2) && !f2[0].is_empty() {
                tmp.push((f2[0], f2[1]));
            }
        }
        camp = HMap::new(tmp.len());
        for (id, ch) in tmp {
            let h = hash_bytes(id);
            if camp.get(h).is_none() {
                camp.insert(h, camp_ids.len() as u32);
                camp_ids.push(id);
                camp_ch.push(ch);
            }
        }
    }
    let ncamp = camp_ids.len();
    let mut camp_direct: Vec<u32> = Vec::new();
    for (i, id) in camp_ids.iter().enumerate() {
        if let Some(v) = canon_camp(id) {
            if v < (1 << 22) {
                if camp_direct.len() <= v {
                    camp_direct.resize(v + 1, u32::MAX);
                }
                camp_direct[v] = i as u32;
            }
        }
    }

    let ctx = Ctx { ws, we, prod_direct, camp_direct, prod, unknown_cat, camp };

    // files
    let ev_files: Vec<&'static [u8]> = list_csv(&data_dir.join("events")).iter().map(|p| map_file(p)).collect();
    let ck_files: Vec<&'static [u8]> = list_csv(&data_dir.join("clicks")).iter().map(|p| map_file(p)).collect();
    let lk_files: Vec<&'static [u8]> = list_csv(&data_dir.join("links")).iter().map(|p| map_file(p)).collect();

    const CH: usize = 4 << 20;
    let mut chunks: Vec<Chunk> = Vec::new();
    for (kind, files) in [(0u8, &ev_files), (1u8, &ck_files)] {
        for &d in files.iter() {
            let mut s = 0;
            while s < d.len() {
                let e = (s + CH).min(d.len());
                chunks.push(Chunk { kind, data: d, start: s, end: e });
                s = e;
            }
        }
    }
    let ev_bytes: usize = ev_files.iter().map(|f| f.len()).sum();
    let est_ev = ev_bytes / 70 + 1024;
    let per_bucket = (est_ev / nthreads / NB_E) * 5 / 4 + 16;

    tick("setup");
    if std::env::var("SS_BENCH").is_ok() {
        let mode: u32 = std::env::var("SS_BENCH").unwrap().parse().unwrap();
        let t = std::time::Instant::now();
        let mut f: [&'static [u8]; 7] = [&[]; 7];
        let mut acc = 0u64;
        for &d in &ev_files {
            let mut pos = memchr::memchr(b'\n', d).map(|p| p + 1).unwrap_or(d.len());
            while pos < d.len() {
                let (ok, end) = if mode >= 1 { split_line::<7>(d, pos, &mut f) } else { (false, memchr::memchr(b'\n', &d[pos..]).map(|x| pos + x).unwrap_or(d.len())) };
                let line = &d[pos..end];
                pos = end + 1;
                if mode >= 1 && ok {
                    if mode >= 2 { acc = acc.wrapping_add(hash_bytes(f[0]) ^ hash_bytes(f[1]) ^ hash_bytes(f[5])); }
                    if mode >= 3 { acc = acc.wrapping_add(parse_ts(f[2]).unwrap_or(0) as u64 ^ parse_ts(f[3]).unwrap_or(0) as u64); }
                    if mode >= 4 { acc = acc.wrapping_add(ctx.prod.get(hash_bytes(f[5])).unwrap_or(0) as u64); }
                } else { acc += line.len() as u64; }
            }
        }
        eprintln!("bench mode {} {:.1} ms acc {}", mode, t.elapsed().as_secs_f64()*1e3, acc);
        return;
    }
    // ---------------- phase 1
    let next = AtomicUsize::new(0);
    let linkmap: Mutex<Option<LinkMap>> = Mutex::new(None);
    let mut p1: Vec<(Vec<Vec<ERec>>, Vec<CRec>)> = Vec::new();
    std::thread::scope(|s| {
        let mut hs = Vec::new();
        for t in 0..nthreads {
            let chunks = &chunks;
            let next = &next;
            let ctx = &ctx;
            let lk_files = &lk_files;
            let linkmap = &linkmap;
            hs.push(s.spawn(move || {
                if t == 0 {
                    let lm = build_links(lk_files);
                    *linkmap.lock().unwrap() = Some(lm);
                }
                let mut eb: Vec<Vec<ERec>> = (0..NB_E).map(|_| Vec::with_capacity(per_bucket)).collect();
                let mut cb: Vec<CRec> = Vec::new();
                loop {
                    let k = next.fetch_add(1, Ordering::Relaxed);
                    if k >= chunks.len() {
                        break;
                    }
                    let c = &chunks[k];
                    if c.kind == 0 {
                        parse_events(c, ctx, &mut eb);
                    } else {
                        parse_clicks(c, ctx, &mut cb);
                    }
                }
                (eb, cb)
            }));
        }
        for h in hs {
            p1.push(h.join().unwrap());
        }
    });
    let linkmap = linkmap.lock().unwrap().take().unwrap();
    tick("phase1 parse");

    // ---------------- phase 2: dedup + person mapping
    let n_items = NB_E + p1.len();
    assert!(p1.len() <= 64);
    let pb_est = est_ev / nthreads / NB_P * 5 / 4 + 8;
    let next = AtomicUsize::new(0);
    let p1r = &p1;
    let lm = &linkmap;
    let mut p2: Vec<(Vec<Vec<PRec>>, Vec<Vec<PClick>>)> = Vec::new();
    std::thread::scope(|s| {
        let mut hs = Vec::new();
        for _t in 0..nthreads {
            let next = &next;
            hs.push(s.spawn(move || {
                let mut pe: Vec<Vec<PRec>> = (0..NB_P).map(|_| Vec::with_capacity(pb_est)).collect();
                let mut pc: Vec<Vec<PClick>> = (0..NB_P).map(|_| Vec::new()).collect();
                let mut tkeys: Vec<u64> = Vec::new();
                let mut tidx: Vec<u32> = Vec::new();
                let mut win: Vec<PRec> = Vec::new();
                loop {
                    let k = next.fetch_add(1, Ordering::Relaxed);
                    if k >= n_items {
                        break;
                    }
                    if k < NB_E {
                        let mut n = 0usize;
                        for (eb, _) in p1r.iter() {
                            n += eb[k].len();
                        }
                        if n == 0 {
                            continue;
                        }
                        let cap = (n * 2).next_power_of_two();
                        let mask = cap - 1;
                        tkeys.clear();
                        tkeys.resize(cap, 0);
                        tidx.clear();
                        tidx.resize(cap, 0);
                        let get = |x: u32| -> &ERec { unsafe { p1r.get_unchecked((x >> 26) as usize).0.get_unchecked(k).get_unchecked((x & ((1 << 26) - 1)) as usize) } };
                        for (ti, (eb, _)) in p1r.iter().enumerate() {
                            for (ri, r) in eb[k].iter().enumerate() {
                                let code = ((ti as u32) << 26) | ri as u32;
                                let mut i = (r.eid as usize) & mask;
                                loop {
                                    let kk = tkeys[i];
                                    if kk == 0 {
                                        tkeys[i] = r.eid;
                                        tidx[i] = code;
                                        break;
                                    }
                                    if kk == r.eid {
                                        if r.ingest < get(tidx[i]).ingest {
                                            tidx[i] = code;
                                        }
                                        break;
                                    }
                                    i = (i + 1) & mask;
                                }
                            }
                        }
                        // collect winners (valid), scanning records sequentially
                        win.clear();
                        for (ti, (eb, _)) in p1r.iter().enumerate() {
                            for (ri, r) in eb[k].iter().enumerate() {
                                if r.valid != 1 {
                                    continue;
                                }
                                let code = ((ti as u32) << 26) | ri as u32;
                                let mut i = (r.eid as usize) & mask;
                                while tkeys[i] != r.eid {
                                    i = (i + 1) & mask;
                                }
                                if tidx[i] == code {
                                    win.push(PRec { person: r.user, ts: r.ts, nref: r.uref, price: r.price, cat: r.cat, typ: r.typ, _pad: 0 });
                                }
                            }
                        }
                        const PF: usize = 12;
                        let nw = win.len();
                        for w in 0..nw {
                            if w + PF < nw {
                                lm.prefetch(unsafe { win.get_unchecked(w + PF) }.person);
                            }
                            let mut r = unsafe { *win.get_unchecked(w) };
                            if let Some((nh, nr)) = lm.get(r.person) {
                                r.person = nh;
                                r.nref = nr;
                            }
                            let b = (r.person >> 53) as usize & (NB_P - 1);
                            unsafe { pe.get_unchecked_mut(b) }.push(r);
                        }
                    } else {
                        let cl = &p1r[k - NB_E].1;
                        const PF: usize = 16;
                        for w in 0..cl.len() {
                            if w + PF < cl.len() {
                                lm.prefetch(cl[w + PF].user);
                            }
                            let c = &cl[w];
                            let person = match lm.get(c.user) {
                                Some((nh, _)) => nh,
                                None => c.user,
                            };
                            let b = (person >> 53) as usize & (NB_P - 1);
                            pc[b].push(PClick { person, ts: c.ts, cref: c.cref, camp: c.camp, _pad: 0 });
                        }
                    }
                }
                (pe, pc)
            }));
        }
        for h in hs {
            p2.push(h.join().unwrap());
        }
    });
    tick("phase2 dedup");
    std::mem::forget(p1);
    tick("drop p1");

    // ---------------- phase 3: sessions + attribution
    let next = AtomicUsize::new(0);
    let p2r = &p2;
    let ncat = cats.len();
    let ctxr = &ctx;
    let user_dir = out_dir.join("user_daily");
    let camp_dir = out_dir.join("campaign_daily");
    let _ = std::fs::create_dir_all(&user_dir);
    let _ = std::fs::create_dir_all(&camp_dir);
    let cats_r = &cats;
    let accs: Mutex<Vec<Acc>> = Mutex::new(Vec::new());
    let mut order: Vec<(usize, usize)> = (0..NB_P).map(|k| (p2.iter().map(|(pe, _)| pe[k].len()).sum::<usize>(), k)).collect();
    order.sort_unstable_by(|a, b| b.0.cmp(&a.0));
    let order: Vec<usize> = order.into_iter().map(|x| x.1).collect();
    let order = &order;
    std::thread::scope(|s| {
        for t in 0..nthreads {
            let next = &next;
            let user_dir = &user_dir;
            let accs = &accs;
            s.spawn(move || {
                let mut acc = Acc::new(ncamp, nd);
                let mut rows: Vec<URow> = Vec::new();
                let mut sc = Scratch {
                    catsum: vec![0; ncat],
                    touched: Vec::new(),
                    starts: Vec::new(),
                    sdays: Vec::new(),
                    open: vec![(i64::MIN, i64::MIN); ncamp + 1],
                    open_list: Vec::new(),
                    day_first: Vec::new(),
                };
                let mut evs: Vec<PRec> = Vec::new();
                let mut cls: Vec<PClick> = Vec::new();
                loop {
                    let k = next.fetch_add(1, Ordering::Relaxed);
                    if k >= NB_P {
                        break;
                    }
                    let k = order[k];
                    evs.clear();
                    cls.clear();
                    for (pe, pc) in p2r.iter() {
                        evs.extend_from_slice(&pe[k]);
                        cls.extend_from_slice(&pc[k]);
                    }
                    if evs.is_empty() {
                        continue;
                    }
                    process_bucket(&mut evs, &mut cls, ctxr, ws_day, ncamp, &mut rows, &mut acc, &mut sc);
                }
                pq::write_user(&user_dir.join(format!("part-{t:03}.parquet")), &rows, cats_r);
                accs.lock().unwrap().push(acc);
            });
        }
    });

    tick("phase3 sessions+write user");
    // merge campaign accumulators
    let accs = accs.into_inner().unwrap();
    let n = (ncamp + 1) * nd;
    let mut tot = Acc::new(ncamp, nd);
    for a in &accs {
        for i in 0..n {
            tot.sess[i] += a.sess[i];
            tot.psess[i] += a.psess[i];
            tot.rev[i] += a.rev[i];
            tot.assist[i] += a.assist[i];
        }
    }
    let mut crow_id: Vec<&'static [u8]> = Vec::new();
    let mut crow_ch: Vec<&'static [u8]> = Vec::new();
    let mut crow: Vec<[i64; 5]> = Vec::new();
    for c in 0..=ncamp {
        for d in 0..nd {
            let i = c * nd + d;
            if tot.sess[i] > 0 || tot.assist[i] > 0 {
                let (id, ch): (&'static [u8], &'static [u8]) = if c == ncamp { (b"organic", b"organic") } else { (camp_ids[c], camp_ch[c]) };
                crow_id.push(id);
                crow_ch.push(ch);
                crow.push([(ws_day + d as i64), tot.sess[i], tot.psess[i], tot.rev[i], tot.assist[i]]);
            }
        }
    }
    pq::write_campaign(&camp_dir.join("part-000.parquet"), &crow_id, &crow_ch, &crow);
    tick("done");
    let _ = (Arc::new(0), SendPtr::<u8>(std::ptr::null_mut()));
    std::process::exit(0);
}
