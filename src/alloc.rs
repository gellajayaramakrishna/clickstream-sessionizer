#![allow(unsafe_op_in_unsafe_fn)]
// Global allocator: big blocks come straight from mmap with transparent huge pages,
// and grow with mremap (no copy). Small blocks use the system allocator.
use std::alloc::{GlobalAlloc, Layout, System};

pub struct HugeAlloc;

const BIG: usize = 4 << 20;
const MID: usize = 64 << 10;
const SLAB: usize = 64 << 20;

// Mid-size blocks are bump-allocated from huge-page slabs and never freed
// (the process is short-lived); this removes most page faults.
static SLAB_LOCK: std::sync::Mutex<(usize, usize)> = std::sync::Mutex::new((0, 0));

unsafe fn mid_alloc(size: usize, align: usize) -> *mut u8 {
    let mut g = match SLAB_LOCK.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    let (mut cur, mut end) = *g;
    let mut p = (cur + align - 1) & !(align - 1);
    if cur == 0 || p + size > end {
        let raw = libc::mmap(
            std::ptr::null_mut(),
            SLAB + HP,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
            -1,
            0,
        );
        if raw == libc::MAP_FAILED {
            return std::ptr::null_mut();
        }
        let base = ((raw as usize) + HP - 1) & !(HP - 1);
        libc::madvise(base as *mut libc::c_void, SLAB, libc::MADV_HUGEPAGE);
        cur = base;
        end = base + SLAB;
        p = (cur + align - 1) & !(align - 1);
    }
    cur = p + size;
    *g = (cur, end);
    p as *mut u8
}

#[inline(always)]
fn is_mid(size: usize, align: usize) -> bool {
    size >= MID && size < BIG && align <= 4096
}
const HP: usize = 2 << 20;

#[inline(always)]
fn round(n: usize) -> usize {
    (n + HP - 1) & !(HP - 1)
}

unsafe fn big_alloc(size: usize) -> *mut u8 {
    let sz = round(size);
    let p = libc::mmap(
        std::ptr::null_mut(),
        sz,
        libc::PROT_READ | libc::PROT_WRITE,
        libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
        -1,
        0,
    );
    if p == libc::MAP_FAILED {
        return std::ptr::null_mut();
    }
    libc::madvise(p, sz, libc::MADV_HUGEPAGE);
    p as *mut u8
}

unsafe impl GlobalAlloc for HugeAlloc {
    #[inline]
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        if is_mid(l.size(), l.align()) {
            return mid_alloc(l.size(), l.align());
        }
        if l.size() >= BIG && l.align() <= 4096 {
            big_alloc(l.size())
        } else {
            System.alloc(l)
        }
    }
    #[inline]
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        if is_mid(l.size(), l.align()) {
            return mid_alloc(l.size(), l.align()); // fresh slab memory is zero
        }
        if l.size() >= BIG && l.align() <= 4096 {
            big_alloc(l.size())
        } else {
            System.alloc_zeroed(l)
        }
    }
    #[inline]
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        if is_mid(l.size(), l.align()) {
            return;
        }
        if l.size() >= BIG && l.align() <= 4096 {
            libc::munmap(p as *mut libc::c_void, round(l.size()));
        } else {
            System.dealloc(p, l)
        }
    }
    #[inline]
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        if l.align() > 4096 {
            return System.realloc(p, l, new);
        }
        let old = l.size();
        let ob = old >= BIG;
        let nb = new >= BIG;
        if is_mid(old, l.align()) || is_mid(new, l.align()) {
            let nl = Layout::from_size_align_unchecked(new, l.align());
            let q = self.alloc(nl);
            if !q.is_null() {
                std::ptr::copy_nonoverlapping(p, q, old.min(new));
                self.dealloc(p, l);
            }
            return q;
        }
        if ob && nb {
            let (os, ns) = (round(old), round(new));
            if os == ns {
                return p;
            }
            let q = libc::mremap(p as *mut libc::c_void, os, ns, libc::MREMAP_MAYMOVE);
            if q == libc::MAP_FAILED {
                return std::ptr::null_mut();
            }
            if ns > os {
                libc::madvise(q, ns, libc::MADV_HUGEPAGE);
            }
            q as *mut u8
        } else if !ob && !nb {
            System.realloc(p, l, new)
        } else {
            let nl = Layout::from_size_align_unchecked(new, l.align());
            let q = self.alloc(nl);
            if !q.is_null() {
                std::ptr::copy_nonoverlapping(p, q, old.min(new));
                self.dealloc(p, l);
            }
            q
        }
    }
}
