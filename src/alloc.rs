#![allow(unsafe_op_in_unsafe_fn)]
// Global allocator: big blocks come straight from mmap with transparent huge pages,
// and grow with mremap (no copy). Small blocks use the system allocator.
use std::alloc::{GlobalAlloc, Layout, System};

pub struct HugeAlloc;

const BIG: usize = 4 << 20;
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
        if l.size() >= BIG && l.align() <= 4096 {
            big_alloc(l.size())
        } else {
            System.alloc(l)
        }
    }
    #[inline]
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        if l.size() >= BIG && l.align() <= 4096 {
            big_alloc(l.size())
        } else {
            System.alloc_zeroed(l)
        }
    }
    #[inline]
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
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
