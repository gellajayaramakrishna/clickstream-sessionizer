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

pub struct Writer<'a, T: Copy> {
    arena: &'a Arena<T>,
    open: Vec<(u32, u16)>,
    filled: Filled,
}

impl<'a, T: Copy> Arena<T> {
    pub fn writer(&'a self) -> Writer<'a, T> {
        Writer {
            arena: self,
            open: vec![(u32::MAX, BLOCK as u16); self.buckets],
            filled: (0..self.buckets).map(|_| Vec::new()).collect(),
        }
    }
}

impl<'a, T: Copy> Writer<'a, T> {
    /// Appends a record to bucket `b` and returns its slot number.
    #[inline(always)]
    pub fn push(&mut self, b: usize, rec: T) -> u32 {
        let o = unsafe { self.open.get_unchecked_mut(b) };
        if o.1 as usize == BLOCK {
            if o.0 != u32::MAX {
                self.filled[b].push((o.0, BLOCK as u16));
            }
            *o = (self.arena.grab(), 0);
        }
        let slot = o.0 as usize * BLOCK + o.1 as usize;
        unsafe { self.arena.base().add(slot).write(rec) };
        o.1 += 1;
        slot as u32
    }

    /// Closes the open blocks and returns what this thread filled.
    pub fn finish(mut self) -> Filled {
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
