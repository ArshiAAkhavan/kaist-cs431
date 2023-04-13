//! Split-ordered linked list.

use core::mem;
use core::sync::atomic::{AtomicUsize, Ordering};
use crossbeam_epoch::{self as epoch, Atomic, Guard, Owned, Shared};
use cs431::lockfree::list::{Cursor, List, Node};
use epoch::unprotected;

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
type SplitOrderedKey = usize;

impl<V> Default for SplitOrderedList<V> {
    fn default() -> Self {
        let list = List::new();
        let buckets = GrowableArray::new();
        let guard = unsafe { &unprotected() };

        //todo sync between bucket pointer and actuall node in the list

        list.harris_herlihy_shavit_insert(0, None, guard);
        let mut cursor = list.head(guard);
        let _ = cursor.find_harris_herlihy_shavit(&0, guard);
        let bucket_zero = buckets.get(0, guard);
        bucket_zero.store(cursor.curr(), Ordering::Release);

        Self {
            list,
            buckets,
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
        let size = self.size.load(Ordering::Acquire);
        let bucket = index % size;
        let so_bucket_key = self.get_so_bucket_key(bucket);

        let mut bucket_raw = self.buckets.get(bucket, guard);
        let node_raw = bucket_raw.load(Ordering::Acquire, &guard);
        match node_raw.is_null() {
            true => self.make_bucket(bucket, size, guard),
            false => Cursor::new(bucket_raw, node_raw),
        }
    }
    fn make_bucket<'s>(
        &'s self,
        bucket: usize,
        size: usize,
        guard: &'s Guard,
    ) -> Cursor<'s, usize, Option<V>> {
        let parent = self.get_parent_bucket(bucket);
        let parent_raw = self.buckets.get(parent, guard);
        let node_raw = parent_raw.load(Ordering::Acquire, &guard);
        if node_raw.is_null() {
            self.make_bucket(parent, size, guard);
        }
        self.insert_bucket(Cursor::new(parent_raw, node_raw), bucket, guard)
    }

    fn get_parent_bucket(&self, bucket: usize) -> usize {
        if bucket == 1 {
            return 0;
        }
        let x = bucket.reverse_bits();
        let y = bucket.leading_zeros() + 1;
        let (z, _) = x.overflowing_shr(y);
        let w = z.reverse_bits();
        w
    }

    fn insert_bucket<'s>(
        &'s self,
        mut parent: Cursor<'s, usize, Option<V>>,
        bucket: usize,
        guard: &'s Guard,
    ) -> Cursor<'s, usize, Option<V>> {
        let mut node = Owned::new(Node::new(self.get_so_bucket_key(bucket), None));
        loop {
            let found = parent
                .find_harris_herlihy_shavit(&self.get_so_bucket_key(bucket), guard)
                .unwrap();
            if found {
                return parent;
            }
            match parent.insert(node, guard) {
                Ok(_) => break,
                Err(n) => node = n,
            }
        }
        parent
    }

    #[inline]
    fn get_so_bucket_key(&self, key: usize) -> SplitOrderedKey {
        key.reverse_bits()
    }

    #[inline]
    fn get_so_data_key(&self, key: usize) -> SplitOrderedKey {
        key.reverse_bits() | 1
    }

    /// Moves the bucket cursor returned from `lookup_bucket` to the position of the given key.
    /// Returns `(size, found, cursor)`
    fn find<'s>(
        &'s self,
        key: &usize,
        guard: &'s Guard,
    ) -> (usize, bool, Cursor<'s, usize, Option<V>>) {
        let mut bucket_cusor = self.lookup_bucket(*key, guard);
        let found = bucket_cusor
            .find_harris(&self.get_so_data_key(*key), guard)
            .unwrap();
        (self.size.load(Ordering::Acquire), found, bucket_cusor)
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
