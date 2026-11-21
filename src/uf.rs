//! Joining linked user IDs into people with all threads at once.
//!
//! IDs are interned into a lock-free open-addressing table; the slot number
//! is the ID's node. Unions link the root with the larger node number under
//! the smaller one with a compare-and-swap, which can never form a cycle, and
//! finds halve their paths as they go. A person's name is the byte-wise
//! smallest of its IDs, taken with an atomic minimum (packed keys compare like
//! their bytes; the rare interned long IDs are settled afterwards by bytes).

use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};

use crate::key::{hash_key, is_long, key_cmp, Key};
use crate::people::People;

struct Table {
    keys: Vec<AtomicU64>,
    /// IDs interned so far.
    count: AtomicUsize,
    /// 0 for a root, else parent node + 1.
    parent: Vec<AtomicU32>,
    name: Vec<AtomicU64>,
    mask: usize,
}

fn zeroed_atomics64(n: usize) -> Vec<AtomicU64> {
    // A zeroed Vec<u64> has the same layout as Vec<AtomicU64>.
    let mut v = std::mem::ManuallyDrop::new(vec![0u64; n]);
    unsafe { Vec::from_raw_parts(v.as_mut_ptr() as *mut AtomicU64, v.len(), v.capacity()) }
}

fn zeroed_atomics32(n: usize) -> Vec<AtomicU32> {
    let mut v = std::mem::ManuallyDrop::new(vec![0u32; n]);
    unsafe { Vec::from_raw_parts(v.as_mut_ptr() as *mut AtomicU32, v.len(), v.capacity()) }
}

impl Table {
    fn intern(&self, k: Key) -> u32 {
        let mut i = hash_key(k) as usize & self.mask;
        loop {
            let cur = self.keys[i].load(Ordering::Acquire);
            if cur == k {
                return i as u32;
            }
            if cur == 0 {
                match self.keys[i].compare_exchange(0, k, Ordering::AcqRel, Ordering::Acquire) {
                    Ok(_) => {
                        self.count.fetch_add(1, Ordering::Relaxed);
                        return i as u32;
                    }
                    Err(now) if now == k => return i as u32,
                    Err(_) => {}
                }
            }
            i = (i + 1) & self.mask;
        }
    }

    fn find(&self, mut x: u32) -> u32 {
        loop {
            let p = self.parent[x as usize].load(Ordering::Acquire);
            if p == 0 {
                return x;
            }
            let p = p - 1;
            let gp = self.parent[p as usize].load(Ordering::Acquire);
            if gp == 0 {
                return p;
            }
            let _ = self.parent[x as usize].compare_exchange(p + 1, gp, Ordering::AcqRel, Ordering::Relaxed);
            x = gp - 1;
        }
    }

    fn union(&self, a: u32, b: u32) {
        loop {
            let ra = self.find(a);
            let rb = self.find(b);
            if ra == rb {
                return;
            }
            let (lo, hi) = if ra < rb { (ra, rb) } else { (rb, ra) };
            if self.parent[hi as usize].compare_exchange(0, lo + 1, Ordering::AcqRel, Ordering::Relaxed).is_ok() {
                return;
            }
        }
    }
}

/// Builds the table from each linked ID to its person's name, using
/// `threads` threads. `parts[t]` are the link pairs parsed by thread t.
pub fn build(parts: &[Vec<(Key, Key)>], threads: usize) -> People {
    let pairs: usize = parts.iter().map(|p| p.len()).sum();
    // At most 2 * pairs IDs, so the load factor stays below 1 (typically ~0.6).
    let size = (pairs * 2 + 16).next_power_of_two();
    let t = Table {
        keys: zeroed_atomics64(size),
        count: AtomicUsize::new(0),
        parent: zeroed_atomics32(size),
        name: zeroed_atomics64(size),
        mask: size - 1,
    };
    let par = |f: &(dyn Fn(usize) + Sync)| {
        std::thread::scope(|s| {
            for i in 1..threads {
                s.spawn(move || f(i));
            }
            f(0);
        });
    };
    // Unions; thread i takes every part p with p % threads == i.
    par(&|i| {
        for p in (i..parts.len()).step_by(threads) {
            for &(a, b) in &parts[p] {
                let ia = t.intern(a);
                if a != b {
                    let ib = t.intern(b);
                    t.union(ia, ib);
                }
            }
        }
    });
    // Names: the smallest key of each person. `name` holds !key so that the
    // zeroed array starts as "no name yet" and fetch_max picks the minimum.
    let range = |i: usize| (size * i / threads, size * (i + 1) / threads);
    let long = std::sync::Mutex::new(Vec::new());
    par(&|i| {
        let (lo, hi) = range(i);
        let mut my_long = Vec::new();
        for s in lo..hi {
            let k = t.keys[s].load(Ordering::Relaxed);
            if k == 0 {
                continue;
            }
            if is_long(k) {
                my_long.push(s as u32);
                continue;
            }
            let r = t.find(s as u32);
            t.name[r as usize].fetch_max(!k, Ordering::Relaxed);
        }
        long.lock().unwrap().extend(my_long);
    });
    // Long IDs compare by their bytes.
    for s in long.into_inner().unwrap() {
        let k = t.keys[s as usize].load(Ordering::Relaxed);
        let r = t.find(s) as usize;
        let cur = t.name[r].load(Ordering::Relaxed);
        if cur == 0 || key_cmp(k, !cur).is_lt() {
            t.name[r].store(!k, Ordering::Relaxed);
        }
    }
    // The lookup table holds only IDs whose person has another name; the
    // number of IDs bounds its size.
    let people = People::with_capacity(t.count.load(Ordering::Relaxed));
    par(&|i| {
        let (lo, hi) = range(i);
        for s in lo..hi {
            let k = t.keys[s].load(Ordering::Relaxed);
            if k == 0 {
                continue;
            }
            let nm = !t.name[t.find(s as u32) as usize].load(Ordering::Relaxed);
            if nm != k {
                people.insert(k, nm);
            }
        }
    });
    people
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key::pack;

    /// Agrees with a plain sequential union-find on random link graphs.
    #[test]
    fn matches_sequential() {
        let mut x: u64 = 12345;
        let mut rnd = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for round in 0..20 {
            let n_ids = 50 + round * 40;
            let ids: Vec<Vec<u8>> = (0..n_ids)
                .map(|i| {
                    if i % 37 == 5 {
                        format!("long-identifier-{i}").into_bytes()
                    } else if i % 11 == 3 {
                        format!("U{i}").into_bytes()
                    } else {
                        format!("u{i}").into_bytes()
                    }
                })
                .collect();
            let mut parts: Vec<Vec<(Key, Key)>> = vec![Vec::new(); 5];
            let mut edges = Vec::new();
            for e in 0..n_ids {
                let a = (rnd() % n_ids as u64) as usize;
                let b = if e % 9 == 0 { a } else { (rnd() % n_ids as u64) as usize };
                edges.push((a, b));
                parts[e % 5].push((pack(&ids[a]), pack(&ids[b])));
            }
            let people = build(&parts, 4);
            // Sequential reference.
            let mut parent: Vec<usize> = (0..n_ids).collect();
            fn f(p: &mut Vec<usize>, x: usize) -> usize {
                let mut r = x;
                while p[r] != r {
                    r = p[r];
                }
                r
            }
            let mut linked = vec![false; n_ids];
            for &(a, b) in &edges {
                linked[a] = true;
                linked[b] = true;
                let (ra, rb) = (f(&mut parent, a), f(&mut parent, b));
                parent[ra] = rb;
            }
            for i in 0..n_ids {
                let r = f(&mut parent, i);
                let best = (0..n_ids).filter(|&j| linked[j] && f(&mut parent, j) == r).map(|j| ids[j].clone()).min().unwrap_or(ids[i].clone());
                let expect = if linked[i] { best } else { ids[i].clone() };
                let got = crate::key::key_bytes(people.get(pack(&ids[i])));
                assert_eq!(got, expect, "round {round} id {}", String::from_utf8_lossy(&ids[i]));
            }
        }
    }
}
