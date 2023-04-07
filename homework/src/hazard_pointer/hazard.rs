use core::marker::PhantomData;
use core::ptr::{self, NonNull};
use std::collections::HashSet;
use std::{fmt, thread};

#[cfg(not(feature = "check-loom"))]
use core::sync::atomic::{fence, AtomicBool, AtomicPtr, AtomicUsize, Ordering};
#[cfg(feature = "check-loom")]
use loom::sync::atomic::{fence, AtomicBool, AtomicPtr, AtomicUsize, Ordering};

use super::HAZARDS;

/// Represents the ownership of a hazard pointer slot.
pub struct Shield<T> {
    slot: NonNull<HazardSlot>,
    _marker: PhantomData<*const T>, // !Send + !Sync
}

impl<T> Shield<T> {
    /// Creates a new shield for hazard pointer.
    pub fn new(hazards: &HazardBag) -> Self {
        let slot = hazards.acquire_slot();
        Self {
            slot: slot.into(),
            _marker: PhantomData,
        }
    }

    /// Try protecting the pointer `*pointer`.
    /// 1. Store `*pointer` to the hazard slot.
    /// 2. Check if `src` still points to `*pointer` (validation) and update `pointer` to the
    ///    latest value.
    /// 3. If validated, return true. Otherwise, clear the slot (store 0) and return false.
    pub fn try_protect(&self, pointer: &mut *const T, src: &AtomicPtr<T>) -> bool {
        unsafe {
            self.slot
                .as_ref()
                .hazard
                .store(*pointer as usize, Ordering::Release)
        };
        fence(Ordering::SeqCst);
        let new_ptr = src.load(Ordering::Relaxed) as *const T;
        if new_ptr == *pointer {
            true
        } else {
            unsafe {
                self.slot.as_ref().hazard.store(0, Ordering::Relaxed);
            }
            *pointer = new_ptr;
            false
        }
    }

    /// Get a protected pointer from `src`.
    pub fn protect(&self, src: &AtomicPtr<T>) -> *const T {
        let mut pointer = src.load(Ordering::Relaxed) as *const T;
        while !self.try_protect(&mut pointer, src) {
            #[cfg(feature = "check-loom")]
            loom::sync::atomic::spin_loop_hint();
        }
        pointer
    }
}

impl<T> Default for Shield<T> {
    fn default() -> Self {
        Self::new(&HAZARDS)
    }
}

impl<T> Drop for Shield<T> {
    /// Clear and release the ownership of the hazard slot.
    fn drop(&mut self) {
        unsafe { self.slot.as_ref().active.store(false, Ordering::Release) }
    }
}

impl<T> fmt::Debug for Shield<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Shield")
            .field("slot address", &self.slot)
            .field("slot data", unsafe { self.slot.as_ref() })
            .finish()
    }
}

/// Global bag (multiset) of hazards pointers.
/// `HazardBag.head` and `HazardSlot.next` form a grow-only list of all hazard slots. Slots are
/// never removed from this list. Instead, it gets deactivated and recycled for other `Shield`s.
#[derive(Debug)]
pub struct HazardBag {
    head: AtomicPtr<HazardSlot>,
}

/// See `HazardBag`
#[derive(Debug)]
struct HazardSlot {
    // Whether this slot is occupied by a `Shield`.
    active: AtomicBool,
    // Machine representation of the hazard pointer.
    hazard: AtomicUsize,
    // Immutable pointer to the next slot in the bag.
    next: *const HazardSlot,
}

impl HazardSlot {
    fn new() -> Self {
        Self {
            active: AtomicBool::new(false),
            hazard: AtomicUsize::new(0),
            next: ptr::null(),
        }
    }
}

impl HazardBag {
    #[cfg(not(feature = "check-loom"))]
    /// Creates a new global hazard set.
    pub const fn new() -> Self {
        Self {
            head: AtomicPtr::new(ptr::null_mut()),
        }
    }

    #[cfg(feature = "check-loom")]
    /// Creates a new global hazard set.
    pub fn new() -> Self {
        Self {
            head: AtomicPtr::new(ptr::null_mut()),
        }
    }

    /// Acquires a slot in the hazard set, either by recyling an inactive slot or allocating a new
    /// slot.
    fn acquire_slot(&self) -> &HazardSlot {
        match self.try_acquire_inactive() {
            Some(slot) => slot,
            None => {
                let mut slot = Box::new(HazardSlot::new());
                slot.active.store(true, Ordering::Relaxed);
                loop {
                    let head = self.head.load(Ordering::Acquire);
                    slot.next = head as *const _;
                    let slot_raw = Box::into_raw(slot);
                    match self.head.compare_exchange(
                        head,
                        slot_raw,
                        Ordering::AcqRel,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => {
                            return unsafe { &*slot_raw };
                        }
                        Err(_) => {
                            slot = unsafe { Box::from_raw(slot_raw) };
                        }
                    }
                }
            }
        }
    }
    /// Find an inactive slot and activate it.
    fn try_acquire_inactive(&self) -> Option<&HazardSlot> {
        let mut curr = self.head.load(Ordering::Acquire);
        while let Some(slot) = unsafe { curr.as_ref() } {
            match slot
                .active
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            {
                Ok(_) => return Some(slot),
                Err(_) => curr = slot.next as *mut HazardSlot,
            }
        }
        None
    }

    /// Returns all the hazards in the set.
    pub fn all_hazards(&self) -> HashSet<usize> {
        let mut hazards = HashSet::new();
        let mut curr = self.head.load(Ordering::Acquire) as *const HazardSlot;
        while let Some(slot) = unsafe { curr.as_ref() } {
            if slot.active.load(Ordering::Acquire) {
                let raw = slot.hazard.load(Ordering::Relaxed);
                if raw != 0 {
                    hazards.insert(raw);
                }
            }

            curr = slot.next;
        }
        hazards
    }
}

impl Drop for HazardBag {
    /// Frees all slots.
    fn drop(&mut self) {
        let mut curr = self.head.load(Ordering::Acquire);
        while let Some(slot) = unsafe { curr.as_ref() } {
            let slot = unsafe { Box::from_raw(curr as *mut HazardSlot) };
            curr = slot.next as *mut _;
        }
    }
}

unsafe impl Send for HazardSlot {}
unsafe impl Sync for HazardSlot {}

#[cfg(all(test, not(feature = "check-loom")))]
mod tests {
    use super::{HazardBag, Shield};
    use std::collections::HashSet;
    use std::mem;
    use std::ops::Range;
    use std::sync::{atomic::AtomicPtr, Arc};
    use std::thread;

    const THREADS: usize = 8;
    const VALUES: Range<usize> = 1..1024;

    // `all_hazards` should return hazards protected by shield(s).
    #[test]
    fn all_hazards_protected() {
        let hazard_bag = Arc::new(HazardBag::new());
        let _ = (0..THREADS)
            .map(|_| {
                let hazard_bag = hazard_bag.clone();
                thread::spawn(move || {
                    for data in VALUES {
                        let src = AtomicPtr::new(data as *mut ());
                        let shield = Shield::new(&hazard_bag);
                        shield.protect(&src);
                        // leak the shield so that
                        mem::forget(shield);
                    }
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|th| th.join().unwrap())
            .collect::<Vec<_>>();
        let all = hazard_bag.all_hazards();
        let values = VALUES.collect();
        let all = hazard_bag.all_hazards();
        assert!(all.is_superset(&values))
    }

    // `all_hazards` should not return values that are no longer protected.
    #[test]
    fn all_hazards_unprotected() {
        let hazard_bag = Arc::new(HazardBag::new());
        let _ = (0..THREADS)
            .map(|_| {
                let hazard_bag = hazard_bag.clone();
                thread::spawn(move || {
                    for data in VALUES {
                        let src = AtomicPtr::new(data as *mut ());
                        let shield = Shield::new(&hazard_bag);
                        shield.protect(&src);
                    }
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|th| th.join().unwrap())
            .collect::<Vec<_>>();
        let all = hazard_bag.all_hazards();
        let values = VALUES.collect();
        let intersection: HashSet<_> = all.intersection(&values).collect();
        assert!(intersection.is_empty())
    }

    // `acquire_slot` should recycle existing slots.
    #[test]
    fn recycle_slots() {
        let hazard_bag = HazardBag::new();
        // allocate slots
        let shields = (0..1024)
            .map(|_| Shield::<()>::new(&hazard_bag))
            .collect::<Vec<_>>();
        // slot addresses
        let old_slots = shields
            .iter()
            .map(|s| s.slot.as_ptr() as usize)
            .collect::<HashSet<_>>();
        // release the slots
        drop(shields);

        let shields = (0..128)
            .map(|_| Shield::<()>::new(&hazard_bag))
            .collect::<Vec<_>>();
        let new_slots = shields
            .iter()
            .map(|s| s.slot.as_ptr() as usize)
            .collect::<HashSet<_>>();

        // no new slots should've been created
        assert!(new_slots.is_subset(&old_slots));
    }
}
