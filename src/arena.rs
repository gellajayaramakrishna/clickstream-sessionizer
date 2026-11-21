//! Bucketed record storage built from fixed-size blocks.
//!
//! One large anonymous mapping is handed out block by block with an atomic
//! bump counter. Each thread keeps one open block per bucket and writes
//! records straight into it, so there is no staging copy, blocks fill the
//! mapping densely (the kernel only zeroes memory that is used), and every
//! record has a stable slot number from the moment it is written.

use std::sync::atomic::{AtomicUsize, Ordering};

use memmap2::MmapMut;

/// Records per block.
pub const BLOCK: usize = 256;

pub struct Arena<T: Copy> {
    mem: MmapMut,
    blocks: usize,
    next: AtomicUsize,
    buckets: usize,
    _t: std::marker::PhantomData<T>,
}

unsafe impl<T: Copy + Send> Sync for Arena<T> {}

/// The blocks one thread filled for each bucket: (block, records in it).
pub type Filled = Vec<Vec<(u32, u16)>>;

impl<T: Copy> Arena<T> {
    /// `max_records` must bound the number of records ever pushed; the
    /// mapping is reserved, not touched, so a generous bound costs nothing.
    pub fn new(max_records: usize, buckets: usize, threads: usize) -> Self {
        let blocks = max_records / BLOCK + buckets * threads + 16;
        let bytes = blocks * BLOCK * std::mem::size_of::<T>();
        let mem = memmap2::MmapOptions::new().len(bytes.max(4096)).map_anon().expect("map arena");
        #[cfg(target_os = "linux")]
        {
            let _ = mem.advise(memmap2::Advice::HugePage);
        }
        Arena { mem, blocks, next: AtomicUsize::new(0), buckets, _t: std::marker::PhantomData }
    }

    #[inline(always)]
    fn base(&self) -> *mut T {
        self.mem.as_ptr() as *mut T
    }

    fn grab(&self) -> u32 {
        let b = self.next.fetch_add(1, Ordering::Relaxed);
        assert!(b < self.blocks, "arena full");
        b as u32
    }

    /// The records of one block.
    #[inline(always)]
    pub fn block(&self, block: u32, n: u16) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.base().add(block as usize * BLOCK), n as usize) }
    }

    /// Slots in use (an upper bound on any slot number).
    pub fn slots(&self) -> usize {
        self.next.load(Ordering::Relaxed) * BLOCK
    }
}

/// Records staged per bucket before they are copied into the arena.
pub const STAGE: usize = 16;

/// A writer that stages records per bucket and copies each full stage into
/// the arena in one run, which streams far better than scattered stores.
/// Each record may carry a value `M` that is handed back, with the record's
/// final slot, when its stage is copied.
pub struct Writer<'a, T: Copy, M: Copy> {
    arena: &'a Arena<T>,
    open: Vec<(u32, u16)>,
    filled: Filled,
    stage: Vec<std::mem::MaybeUninit<T>>,
    meta: Vec<std::mem::MaybeUninit<M>>,
    lens: Vec<u8>,
}

impl<'a, T: Copy> Arena<T> {
    pub fn writer<M: Copy>(&'a self) -> Writer<'a, T, M> {
        Writer {
            arena: self,
            open: vec![(u32::MAX, BLOCK as u16); self.buckets],
            filled: (0..self.buckets).map(|_| Vec::new()).collect(),
            stage: vec![std::mem::MaybeUninit::uninit(); self.buckets * STAGE],
            meta: vec![std::mem::MaybeUninit::uninit(); self.buckets * STAGE],
            lens: vec![0; self.buckets],
        }
    }
}

impl<'a, T: Copy, M: Copy> Writer<'a, T, M> {
    /// Stages a record for bucket `b`. When the stage fills, its records are
    /// copied into the arena and `done(slot, meta)` is called for each.
    #[inline(always)]
    pub fn push<F: FnMut(u32, M)>(&mut self, b: usize, rec: T, meta: M, done: &mut F) {
        let len = self.lens[b] as usize;
        unsafe {
            self.stage.get_unchecked_mut(b * STAGE + len).write(rec);
            self.meta.get_unchecked_mut(b * STAGE + len).write(meta);
        }
        if len + 1 == STAGE {
            self.flush(b, STAGE, done);
            self.lens[b] = 0;
        } else {
            self.lens[b] = (len + 1) as u8;
        }
    }

    #[inline(never)]
    fn flush<F: FnMut(u32, M)>(&mut self, b: usize, n: usize, done: &mut F) {
        let mut copied = 0;
        while copied < n {
            let o = &mut self.open[b];
            if o.1 as usize == BLOCK {
                if o.0 != u32::MAX {
                    self.filled[b].push((o.0, BLOCK as u16));
                }
                *o = (self.arena.grab(), 0);
            }
            let room = BLOCK - o.1 as usize;
            let take = room.min(n - copied);
            let slot = o.0 as usize * BLOCK + o.1 as usize;
            unsafe {
                std::ptr::copy_nonoverlapping(
                    self.stage.as_ptr().add(b * STAGE + copied) as *const T,
                    self.arena.base().add(slot),
                    take,
                );
            }
            for i in 0..take {
                let m = unsafe { self.meta[b * STAGE + copied + i].assume_init() };
                done((slot + i) as u32, m);
            }
            o.1 += take as u16;
            copied += take;
        }
    }

    /// Copies the partial stages, closes the open blocks and returns what
    /// this thread filled.
    pub fn finish<F: FnMut(u32, M)>(mut self, done: &mut F) -> Filled {
        for b in 0..self.lens.len() {
            let n = self.lens[b] as usize;
            if n > 0 {
                self.flush(b, n, done);
                self.lens[b] = 0;
            }
        }
        for b in 0..self.open.len() {
            let (blk, n) = self.open[b];
            if blk != u32::MAX && n > 0 {
                self.filled[b].push((blk, n));
            }
        }
        self.filled
    }
}

/// Merges the per-thread block lists into one list per bucket.
pub fn merge(parts: Vec<Filled>, buckets: usize) -> Vec<Vec<(u32, u16)>> {
    let mut out: Vec<Vec<(u32, u16)>> = (0..buckets).map(|_| Vec::new()).collect();
    for p in parts {
        for (b, v) in p.into_iter().enumerate() {
            out[b].extend(v);
        }
    }
    out
}
