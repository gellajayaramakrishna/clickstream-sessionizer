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
        let cap = per + per / 8 + 4096;
        let bytes = (cap * buckets * std::mem::size_of::<T>()).max(4096);
        let mem = MmapMut::map_anon(bytes).expect("map shuffle memory");
        #[cfg(target_os = "linux")]
        {
            let _ = mem.advise(memmap2::Advice::HugePage);
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

    fn put(&self, b: usize, items: &[T]) {
        let n = items.len();
        let at = self.cursors[b].fetch_add(n, Ordering::Relaxed);
        let fit = if at >= self.cap { 0 } else { (self.cap - at).min(n) };
        if fit > 0 {
            unsafe { std::ptr::copy_nonoverlapping(items.as_ptr(), self.region(b).add(at), fit) };
        }
        if fit < n {
            self.overflow[b].lock().unwrap().extend_from_slice(&items[fit..]);
        }
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
        self.sh.put(b, items);
    }

    pub fn finish(mut self) {
        for b in 0..self.sh.buckets {
            let n = self.lens[b] as usize;
            if n > 0 {
                self.flush_bucket(b, n);
                self.lens[b] = 0;
            }
        }
    }
}
