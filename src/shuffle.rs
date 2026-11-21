//! A bucketed shuffle buffer shared by all threads.
//!
//! One large anonymous mapping (huge pages where the kernel allows) is split
//! into a fixed region per bucket. Each thread stages a few records per bucket
//! locally and moves them into the bucket's region in one copy, reserving the
//! space with an atomic cursor, so every bucket ends up contiguous. Records
//! that do not fit their region go to a per-bucket overflow vector.

use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use memmap2::MmapMut;

const STAGE: usize = 32;

pub struct Shuffle<T: Copy> {
    mem: MmapMut,
    cap: usize,
    buckets: usize,
    cursors: Vec<AtomicUsize>,
    overflow: Vec<Mutex<Vec<T>>>,
    _t: PhantomData<T>,
}

unsafe impl<T: Copy + Send> Sync for Shuffle<T> {}

impl<T: Copy> Shuffle<T> {
    /// `expected` is an estimate of the total record count.
    pub fn new(expected: usize, buckets: usize) -> Self {
        let per = expected / buckets;
        // A multiple of STAGE records, so every full flush lands on a
        // 64-byte boundary and can use streaming stores.
        let cap = (per + per / 8 + 4096).div_ceil(STAGE) * STAGE;
        let bytes = (cap * buckets * std::mem::size_of::<T>()).max(4096);
        let mem = MmapMut::map_anon(bytes).expect("map shuffle memory");
        #[cfg(target_os = "linux")]
        {
            // Huge pages left to the kernel default (experiment: no MADV_HUGEPAGE).
        }
        Shuffle {
            mem,
            cap,
            buckets,
            cursors: (0..buckets).map(|_| AtomicUsize::new(0)).collect(),
            overflow: (0..buckets).map(|_| Mutex::new(Vec::new())).collect(),
            _t: PhantomData,
        }
    }

    #[inline(always)]
    fn region(&self, b: usize) -> *mut T {
        unsafe { (self.mem.as_ptr() as *mut T).add(b * self.cap) }
    }

    /// Moves one full stage (STAGE records) into bucket `b`.
    #[inline(always)]
    fn put_full(&self, b: usize, items: &[T]) {
        let at = self.cursors[b].fetch_add(STAGE, Ordering::Relaxed);
        if at + STAGE <= self.cap {
            let dst = unsafe { self.region(b).add(at) } as *mut u8;
            let src = items.as_ptr() as *const u8;
            let bytes = STAGE * std::mem::size_of::<T>();
            stream_copy(dst, src, bytes);
        } else {
            // The region is full: undo nothing (readers clamp to cap) and
            // keep the records aside.
            self.overflow[b].lock().unwrap().extend_from_slice(items);
        }
    }

    /// A partial stage at the end goes to the overflow list, so the region
    /// only ever holds whole, aligned stages.
    fn put_partial(&self, b: usize, items: &[T]) {
        self.overflow[b].lock().unwrap().extend_from_slice(items);
    }

    pub fn writer(&self) -> Writer<'_, T> {
        Writer {
            sh: self,
            stage: vec![std::mem::MaybeUninit::uninit(); self.buckets * STAGE],
            lens: vec![0u8; self.buckets],
        }
    }

    /// The records of bucket `b`: its region and its overflow.
    pub fn bucket(&self, b: usize) -> (&[T], Vec<T>) {
        let n = self.cursors[b].load(Ordering::Relaxed).min(self.cap);
        debug_assert!(n % STAGE == 0);
        let main = unsafe { std::slice::from_raw_parts(self.region(b), n) };
        let extra = std::mem::take(&mut *self.overflow[b].lock().unwrap());
        (main, extra)
    }
}

pub struct Writer<'a, T: Copy> {
    sh: &'a Shuffle<T>,
    stage: Vec<std::mem::MaybeUninit<T>>,
    lens: Vec<u8>,
}

impl<'a, T: Copy> Writer<'a, T> {
    #[inline(always)]
    pub fn push(&mut self, b: usize, item: T) {
        let len = self.lens[b] as usize;
        unsafe { self.stage.get_unchecked_mut(b * STAGE + len).write(item) };
        if len + 1 == STAGE {
            self.flush_bucket(b, STAGE);
            self.lens[b] = 0;
        } else {
            self.lens[b] = (len + 1) as u8;
        }
    }

    #[inline(never)]
    fn flush_bucket(&mut self, b: usize, n: usize) {
        let items = unsafe { std::slice::from_raw_parts(self.stage.as_ptr().add(b * STAGE) as *const T, n) };
        if n == STAGE {
            self.sh.put_full(b, items);
        } else {
            self.sh.put_partial(b, items);
        }
    }

    pub fn finish(mut self) {
        for b in 0..self.sh.buckets {
            let n = self.lens[b] as usize;
            if n > 0 {
                self.flush_bucket(b, n);
                self.lens[b] = 0;
            }
        }
        fence();
    }
}

/// Copies `bytes` (a multiple of 64) to a 64-byte aligned `dst` with
/// non-temporal stores: no read of the destination line first, and the
/// shuffle does not push useful data out of the caches.
#[inline(always)]
fn stream_copy(dst: *mut u8, src: *const u8, bytes: usize) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    unsafe {
        use std::arch::x86_64::*;
        if (dst as usize) % 32 == 0 && bytes % 32 == 0 {
            let mut i = 0;
            while i < bytes {
                let v = _mm256_loadu_si256(src.add(i) as *const __m256i);
                _mm256_stream_si256(dst.add(i) as *mut __m256i, v);
                i += 32;
            }
            return;
        }
    }
    unsafe { std::ptr::copy_nonoverlapping(src, dst, bytes) };
}

/// Orders the streaming stores before the threads hand over.
fn fence() {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::x86_64::_mm_sfence();
    }
}
