//! A flat open-addressing table from a linked user ID to its person's name.
//! Lookups can be prefetched in batches, which hides most memory latency.

use crate::key::{hash_key, Key};

pub struct People {
    mem: memmap2::MmapMut,
    mask: usize,
    /// A two-bit Bloom filter over the table's keys (16 bits per key): most
    /// IDs that are not in the table are answered without touching it.
    filter: Vec<std::sync::atomic::AtomicU64>,
    fmask: usize,
}

/// The two filter bit positions of a key.
#[inline(always)]
fn fbits(k: Key, fmask: usize) -> (usize, usize) {
    let h = hash_key(k);
    ((h >> 20) as usize & fmask, (h >> 42) as usize & fmask)
}

impl People {
    /// An empty table for up to `n` entries, filled with `insert` from many
    /// threads at once.
    pub fn with_capacity(n: usize) -> Self {
        let size = (n * 4 / 3 + 1).next_power_of_two().max(16);
        let mem = memmap2::MmapMut::map_anon(size * 16).expect("map people");
        #[cfg(target_os = "linux")]
        {
            let _ = mem.advise(memmap2::Advice::HugePage);
        }
        let fbits_n = (n * 16 + 64).next_power_of_two();
        let filter = (0..fbits_n / 64).map(|_| std::sync::atomic::AtomicU64::new(0)).collect();
        People { mem, mask: size - 1, filter, fmask: fbits_n - 1 }
    }

    #[inline(always)]
    fn maybe(&self, k: Key) -> bool {
        use std::sync::atomic::Ordering;
        let (a, b) = fbits(k, self.fmask);
        let wa = unsafe { self.filter.get_unchecked(a / 64) }.load(Ordering::Relaxed);
        let wb = unsafe { self.filter.get_unchecked(b / 64) }.load(Ordering::Relaxed);
        (wa >> (a % 64)) & (wb >> (b % 64)) & 1 != 0
    }

    /// Adds `k -> v`; safe to call from many threads for distinct keys.
    /// Lookups may only start after all inserts are done.
    pub fn insert(&self, k: Key, v: Key) {
        use std::sync::atomic::{AtomicU64, Ordering};
        let (a, b) = fbits(k, self.fmask);
        self.filter[a / 64].fetch_or(1 << (a % 64), Ordering::Relaxed);
        self.filter[b / 64].fetch_or(1 << (b % 64), Ordering::Relaxed);
        let base = self.mem.as_ptr() as *const [AtomicU64; 2];
        let mut i = hash_key(k) as usize & self.mask;
        loop {
            let slot = unsafe { &*base.add(i) };
            match slot[0].compare_exchange(0, k, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => {
                    slot[1].store(v, Ordering::Release);
                    return;
                }
                Err(cur) if cur == k => {
                    slot[1].store(v, Ordering::Release);
                    return;
                }
                Err(_) => i = (i + 1) & self.mask,
            }
        }
    }

    #[inline(always)]
    fn slots(&self) -> *const [Key; 2] {
        self.mem.as_ptr() as *const [Key; 2]
    }

    #[inline(always)]
    pub fn prefetch(&self, k: Key) {
        if !self.maybe(k) {
            return;
        }
        let i = hash_key(k) as usize & self.mask;
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
            _mm_prefetch(self.slots().add(i) as *const i8, _MM_HINT_T0);
        }
    }

    /// The person's name for ID `k` (keys are never 0).
    #[inline(always)]
    pub fn get(&self, k: Key) -> Key {
        if !self.maybe(k) {
            return k;
        }
        let mut i = hash_key(k) as usize & self.mask;
        loop {
            let s = unsafe { &*self.slots().add(i) };
            if s[0] == k {
                return s[1];
            }
            if s[0] == 0 {
                return k;
            }
            i = (i + 1) & self.mask;
        }
    }
}
