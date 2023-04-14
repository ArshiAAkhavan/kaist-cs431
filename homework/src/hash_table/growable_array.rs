//! Growable array.

use core::fmt::Debug;
use core::marker::PhantomData;
use core::mem;
use core::ops::{Deref, DerefMut};
use core::sync::atomic::{AtomicUsize, Ordering};
use crossbeam_epoch::{self as epoch, unprotected, Atomic, Guard, Owned, Pointer, Shared};
use epoch::CompareExchangeError;

/// Growable array of `Atomic<T>`.
///
/// This is more complete version of the dynamic sized array from the paper. In the paper, the
/// segment table is an array of arrays (segments) of pointers to the elements. In this
/// implementation, a segment contains the pointers to the elements **or other segments**. In other
/// words, it is a tree that has segments as internal nodes.
///
/// # Example run
///
/// Suppose `SEGMENT_LOGSIZE = 3` (segment size 8).
///
/// When a new `GrowableArray` is created, `root` is initialized with `Atomic::null()`.
///
/// ```text
///                          +----+
///                          |root|
///                          +----+
/// ```
///
/// When you store element `cat` at the index `0b001`, it first initializes a segment.
///
/// ```text
///
///                          +----+
///                          |root|
///                          +----+
///                            | height: 1
///                            v
///                 +---+---+---+---+---+---+---+---+
///                 |111|110|101|100|011|010|001|000|
///                 +---+---+---+---+---+---+---+---+
///                                           |
///                                           v
///                                         +---+
///                                         |cat|
///                                         +---+
/// ```
///
/// When you store `fox` at `0b111011`, it is clear that there is no room for indices larger than
/// `0b111`. So it first allocates another segment for upper 3 bits and moves the previous root
/// segment (`0b000XXX` segment) under the `0b000XXX` branch of the the newly allocated segment.
///
/// ```text
///                          +----+
///                          |root|
///                          +----+
///                            | height: 2
///                            v
///                 +---+---+---+---+---+---+---+---+
///                 |111|110|101|100|011|010|001|000|
///                 +---+---+---+---+---+---+---+---+
///                                               |
///                                               v
///                                      +---+---+---+---+---+---+---+---+
///                                      |111|110|101|100|011|010|001|000|
///                                      +---+---+---+---+---+---+---+---+
///                                                                |
///                                                                v
///                                                              +---+
///                                                              |cat|
///                                                              +---+
/// ```
///
/// And then, it allocates another segment for `0b111XXX` indices.
///
/// ```text
///                          +----+
///                          |root|
///                          +----+
///                            | height: 2
///                            v
///                 +---+---+---+---+---+---+---+---+
///                 |111|110|101|100|011|010|001|000|
///                 +---+---+---+---+---+---+---+---+
///                   |                           |
///                   v                           v
/// +---+---+---+---+---+---+---+---+    +---+---+---+---+---+---+---+---+
/// |111|110|101|100|011|010|001|000|    |111|110|101|100|011|010|001|000|
/// +---+---+---+---+---+---+---+---+    +---+---+---+---+---+---+---+---+
///                   |                                            |
///                   v                                            v
///                 +---+                                        +---+
///                 |fox|                                        |cat|
///                 +---+                                        +---+
/// ```
///
/// Finally, when you store `owl` at `0b000110`, it traverses through the `0b000XXX` branch of the
/// level-1 segment and arrives at its 0b110` leaf.
///
/// ```text
///                          +----+
///                          |root|
///                          +----+
///                            | height: 2
///                            v
///                 +---+---+---+---+---+---+---+---+
///                 |111|110|101|100|011|010|001|000|
///                 +---+---+---+---+---+---+---+---+
///                   |                           |
///                   v                           v
/// +---+---+---+---+---+---+---+---+    +---+---+---+---+---+---+---+---+
/// |111|110|101|100|011|010|001|000|    |111|110|101|100|011|010|001|000|
/// +---+---+---+---+---+---+---+---+    +---+---+---+---+---+---+---+---+
///                   |                        |                   |
///                   v                        v                   v
///                 +---+                    +---+               +---+
///                 |fox|                    |owl|               |cat|
///                 +---+                    +---+               +---+
/// ```
///
/// When the array is dropped, only the segments are dropped and the **elements must not be
/// dropped/deallocated**.
///
/// ```text
///                 +---+                    +---+               +---+
///                 |fox|                    |owl|               |cat|
///                 +---+                    +---+               +---+
/// ```
///
/// Instead, it should be handled by the container that the elements actually belong to. For
/// example in `SplitOrderedList`, destruction of elements are handled by `List`.
///
#[derive(Debug)]
pub struct GrowableArray<T> {
    root: Atomic<Segment>,
    _marker: PhantomData<T>,
}

const SEGMENT_LOGSIZE: usize = 10;

struct Segment {
    /// `AtomicUsize` here means `Atomic<T>` or `Atomic<Segment>`.
    inner: [AtomicUsize; 1 << SEGMENT_LOGSIZE],
}

impl Segment {
    fn new() -> Self {
        Self {
            inner: unsafe { mem::zeroed() },
        }
    }
}

impl Deref for Segment {
    type Target = [AtomicUsize; 1 << SEGMENT_LOGSIZE];

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for Segment {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Debug for Segment {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "Segment")
    }
}

impl<T> Drop for GrowableArray<T> {
    /// Deallocate segments, but not the individual elements.
    fn drop(&mut self) {
        fn drop_segment(root_atomic: &Atomic<Segment>, guard: &Guard) {
            let root = root_atomic.load(Ordering::Acquire, guard);
            let height = root.tag();
            if height == 0 {
                return;
            }

            let root = unsafe { root.into_owned() };
            // leaf segment
            if height == 1 {
                return;
            }
            for index in &root.inner {
                let segment = unsafe { &*((index) as *const _ as *const Atomic<Segment>) };
                drop_segment(segment, guard);
            }
        }

        let guard = &unsafe { epoch::unprotected() };
        drop_segment(&self.root, guard);
    }
}

impl<T> Default for GrowableArray<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> GrowableArray<T> {
    /// Create a new growable array.
    pub fn new() -> Self {
        Self {
            root: Atomic::null(),
            _marker: PhantomData,
        }
    }

    // unsafe { &*(root_atomic as *const _ as *const Atomic<T>) }
    /// Returns the reference to the `Atomic` pointer at `index`. Allocates new segments if
    /// necessary.
    pub fn get(&self, mut index: usize, guard: &Guard) -> &Atomic<T> {
        let guard = &epoch::pin();
        let mut root_atomic = &self.root;
        loop {
            let mut root = root_atomic.load(Ordering::Acquire, guard);
            while index >= (1 << (root.tag() * SEGMENT_LOGSIZE)) {
                let segment = Owned::new(Segment::new()).with_tag(root.tag() + 1);
                unsafe {
                    let atomic_index =
                        &*((&segment.inner[0]) as *const _ as *const Atomic<Segment>);
                    atomic_index.store(root, Ordering::Release);
                }
                root = match root_atomic.compare_exchange(
                    root,
                    segment,
                    Ordering::Release,
                    Ordering::Relaxed,
                    guard,
                ) {
                    Ok(new) => new,
                    Err(CompareExchangeError { current: c, .. }) => c,
                }
            }
            if root.tag() == 0 && index == 0 {
                return unsafe { &*(root_atomic as *const _ as *const Atomic<T>) };
            }
            let mut index_height = root.tag();

            // root is not null
            unsafe {
                let atomic =
                    &(*root.as_raw()).inner[index >> ((index_height - 1) * SEGMENT_LOGSIZE)];
                root_atomic = &*(atomic as *const _ as *const Atomic<Segment>);
            }
            index %= 1 << ((index_height - 1) * SEGMENT_LOGSIZE);
        }
    }
}
