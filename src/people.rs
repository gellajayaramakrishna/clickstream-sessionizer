//! A flat open-addressing table from a linked user ID to its person's name.
//! Lookups can be prefetched in batches, which hides most memory latency.

use crate::key::{hash_key, Key};

pub struct People {
    mem: memmap2::MmapMut,
    mask: usize,
}

impl People {
    pub fn new(pairs: &[(Key, Key)]) -> Self {
        // Load factor up to 0.75 keeps the table small enough to stay mostly in cache.
        let size = (pairs.len() * 4 / 3 + 1).next_power_of_two().max(16);
        let mut mem = memmap2::MmapMut::map_anon(size * 16).expect("map people");
        #[cfg(target_os = "linux")]
        {
            let _ = mem.advise(memmap2::Advice::HugePage);
        }
        let slots: &mut [[Key; 2]] = unsafe { std::slice::from_raw_parts_mut(mem.as_mut_ptr() as *mut [Key; 2], size) };
        let mask = size - 1;
        for &(k, v) in pairs {
            let mut i = hash_key(k) as usize & mask;
            while slots[i][0] != 0 && slots[i][0] != k {
                i = (i + 1) & mask;
            }
            slots[i] = [k, v];
        }
        People { mem, mask }
    }

    #[inline(always)]
    fn slots(&self) -> *const [Key; 2] {
        self.mem.as_ptr() as *const [Key; 2]
    }

    pub fn prefetch(&self, k: Key) {
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
