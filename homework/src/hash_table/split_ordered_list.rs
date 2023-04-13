//! Split-ordered linked list.

use core::mem;
use core::sync::atomic::{AtomicUsize, Ordering};
use crossbeam_epoch::{self as epoch, Atomic, Guard, Owned, Shared};
use cs431::lockfree::list::{Cursor, List, Node};

use super::growable_array::GrowableArray;
use crate::map::NonblockingMap;

/// Lock-free map from `usize` in range [0, 2^63-1] to `V`.
///
/// NOTE: We don't care about hashing in this homework for simplicity.
#[derive(Debug)]
pub struct SplitOrderedList<V> {
    /// Lock-free list sorted by recursive-split order. Use `None` sentinel node value.
    list: List<usize, Option<V>>,
    /// array of pointers to the buckets
    buckets: GrowableArray<Node<usize, Option<V>>>,
    /// number of buckets
    size: AtomicUsize,
    /// number of items
    count: AtomicUsize,
}

impl<V> Default for SplitOrderedList<V> {
    fn default() -> Self {
        Self {
            list: List::new(),
            buckets: GrowableArray::new(),
            size: AtomicUsize::new(2),
            count: AtomicUsize::new(0),
        }
    }
}

impl<V> SplitOrderedList<V> {
    /// `size` is doubled when `count > size * LOAD_FACTOR`.
    const LOAD_FACTOR: usize = 2;

    /// Creates a new split ordered list.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a cursor and moves it to the bucket for the given index.  If the bucket doesn't
    /// exist, recursively initializes the buckets.
    fn lookup_bucket<'s>(&'s self, index: usize, guard: &'s Guard) -> Cursor<'s, usize, Option<V>> {
        let bucket_key = index % (1 << self.size.load(Ordering::Acquire));
        let mut bucket_raw = self.buckets.get(bucket_key, guard);
        let node_raw = bucket_raw.load(Ordering::Acquire, &guard);
        if node_raw.is_null() {
            let mut curr = self.lookup_bucket(
                index % (1 << (self.size.load(Ordering::Acquire) - 1)),
                guard,
            );
            let found = curr
                .find_harris_herlihy_shavit(&(index % (self.size.load(Ordering::Acquire))), guard)
                .unwrap();

            let mut n = Owned::new(Node::new(
                index % (1 << (self.size.load(Ordering::Acquire) - 1)),
                None,
            ));
            loop {
                n = match curr.insert(n, guard) {
                    Ok(_) => break,
                    Err(n) => n,
                };
            }
            self.lookup_bucket(index, guard)
        } else {
            let x = AtomicUsize::new(0);
            let y = unsafe { &*(&x as *const _ as *const Atomic<Node<usize, Option<V>>>) };

            Cursor::new(y, node_raw)
        }
    }

    /// Moves the bucket cursor returned from `lookup_bucket` to the position of the given key.
    /// Returns `(size, found, cursor)`
    fn find<'s>(
        &'s self,
        key: &usize,
        guard: &'s Guard,
    ) -> (usize, bool, Cursor<'s, usize, Option<V>>) {
        // let bucket_curr=self.lookup_bucket(index, guard)
        todo!()
    }

    fn assert_valid_key(key: usize) {
        assert!(key.leading_zeros() != 0);
    }
}

impl<V> NonblockingMap<usize, V> for SplitOrderedList<V> {
    fn lookup<'a>(&'a self, key: &usize, guard: &'a Guard) -> Option<&'a V> {
        Self::assert_valid_key(*key);
        let (_, found, cursor) = self.find(key, guard);
        match found {
            true => cursor.lookup()?.into(),
            false => None,
        }
    }

    fn insert(&self, key: &usize, value: V, guard: &Guard) -> Result<(), V> {
        Self::assert_valid_key(*key);
        let (size, found, mut cursor) = self.find(key, guard);
        match found {
            true => Err(value),
            false => {
                let node = Owned::new(Node::new(*key, Some(value)));
                cursor
                    .insert(node, guard)
                    .map_err(|n| n.into_box().into_value().unwrap())
            }
        }
    }

    fn delete<'a>(&'a self, key: &usize, guard: &'a Guard) -> Result<&'a V, ()> {
        Self::assert_valid_key(*key);
        let (size, found, cursor) = self.find(key, guard);
        match found {
            true => cursor.delete(guard).map(|v| v.as_ref().unwrap()),
            false => Err(()),
        }
    }
}
