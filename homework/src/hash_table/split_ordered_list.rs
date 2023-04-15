//! Split-ordered linked list.

use core::mem;
use core::sync::atomic::{AtomicUsize, Ordering};
use crossbeam_epoch::{self as epoch, Atomic, Guard, Owned, Shared};
use cs431::lockfree::list::{Cursor, List, Node};
use epoch::unprotected;
use std::fmt::Debug;

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

        // 0 dummy node
        list.harris_herlihy_shavit_insert(0, None, guard);
        let mut cursor = list.head(guard);
        let _ = cursor.find_harris_herlihy_shavit(&0, guard);
        let bucket_zero = buckets.get(0, guard);
        bucket_zero.store(cursor.curr(), Ordering::Release);

        // 1 dummy node
        list.harris_herlihy_shavit_insert(Self::get_so_bucket_key(1), None, guard);
        let mut cursor = list.head(guard);
        let _ = cursor.find_harris_herlihy_shavit(&Self::get_so_bucket_key(1), guard);
        let bucket_one = buckets.get(1, guard);
        bucket_one.store(cursor.curr(), Ordering::Release);

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
        let size = self.size.load(Ordering::Relaxed);
        let bucket = index % size;

        let mut bucket_raw = self.buckets.get(bucket, guard);
        let node_raw = bucket_raw.load(Ordering::Acquire, guard);
        if node_raw.is_null() {
            self.make_bucket(bucket, size, guard);
        }
        self.get_cursor_to_bucket(bucket, bucket_raw, guard)
    }

    fn make_bucket<'s>(&'s self, bucket: usize, size: usize, guard: &'s Guard) {
        let parent = self.get_parent_bucket(bucket);
        let parent_raw = self.buckets.get(parent, guard);
        let node_raw = parent_raw.load(Ordering::Acquire, guard);
        if node_raw.is_null() {
            self.make_bucket(parent, size, guard);
        }

        let cursor = self.get_cursor_to_bucket(parent, parent_raw, guard);
        self.insert_bucket(cursor, bucket, guard);
    }

    fn insert_bucket<'s>(
        &'s self,
        mut cursor: Cursor<'s, usize, Option<V>>,
        bucket: usize,
        guard: &'s Guard,
    ) {
        let bucket_key = Self::get_so_bucket_key(bucket);
        let mut node = Owned::new(Node::new(bucket_key, None));
        loop {
            let bucket_atomic = self.buckets.get(bucket, guard);
            let bucket_raw = bucket_atomic.load(Ordering::Acquire, guard);
            if !bucket_raw.is_null() {
                let _ = node.into_box();
                return;
            }

            if cursor
                .find_harris_michael(&bucket_key, guard)
                .unwrap_or(false)
            {
                let _ = node.into_box();
                return;
            }
            match cursor.insert(node, guard) {
                Ok(_) => {
                    bucket_atomic.store(cursor.curr(), Ordering::Release);
                    break;
                }
                Err(n) => node = n,
            }
        }
    }

    #[inline]
    fn get_cursor_to_bucket<'g>(
        &'g self,
        bucket: usize,
        bucket_raw: &'g Atomic<Node<usize, Option<V>>>,
        guard: &'g Guard,
    ) -> Cursor<'g, usize, Option<V>> {
        let node_raw = bucket_raw.load(Ordering::Acquire, guard);
        let mut cursor = Cursor::new(bucket_raw, node_raw);
        let _ = cursor.find_harris_michael(&(Self::get_so_bucket_key(bucket) + 1), guard);
        cursor
    }

    #[inline]
    fn get_parent_bucket(&self, bucket: usize) -> usize {
        bucket ^ (1 << (mem::size_of::<usize>() * 8 - bucket.leading_zeros() as usize - 1))
    }

    #[inline]
    fn get_so_bucket_key(key: usize) -> SplitOrderedKey {
        key.reverse_bits()
    }

    #[inline]
    fn get_so_data_key(key: usize) -> SplitOrderedKey {
        key.reverse_bits() | 1
    }

    /// Moves the bucket cursor returned from `lookup_bucket` to the position of the given key.
    /// Returns `(size, found, cursor)`
    fn find<'s>(&'s self, key: &usize, guard: &'s Guard) -> (bool, Cursor<'s, usize, Option<V>>) {
        let mut bucket_cursor = self.lookup_bucket(*key, guard);

        let found = bucket_cursor
            .find_harris_michael(&Self::get_so_data_key(*key), guard)
            .unwrap_or(false);

        (found, bucket_cursor)
    }

    fn assert_valid_key(key: usize) {
        assert!(key.leading_zeros() != 0);
    }
}

impl<V> NonblockingMap<usize, V> for SplitOrderedList<V> {
    fn lookup<'a>(&'a self, key: &usize, guard: &'a Guard) -> Option<&'a V> {
        Self::assert_valid_key(*key);
        let (found, cursor) = self.find(key, guard);
        match found {
            true => cursor.lookup()?.into(),
            false => None,
        }
    }

    fn insert(&self, key: &usize, value: V, guard: &Guard) -> Result<(), V> {
        Self::assert_valid_key(*key);
        let (found, mut cursor) = self.find(key, guard);
        if found {
            return Err(value);
        }

        let mut node = Owned::new(Node::new(Self::get_so_data_key(*key), Some(value)));
        match cursor.insert(node, guard) {
            Ok(_) => {
                let prev_count = self.count.fetch_add(1, Ordering::Relaxed) + 1;
                let prev_size = self.size.load(Ordering::Relaxed);
                if prev_count > prev_size * Self::LOAD_FACTOR {
                    // we don't care about the results, both way, we win!
                    let _ = self.size.compare_exchange(
                        prev_size,
                        prev_size * 2,
                        Ordering::Release,
                        Ordering::Relaxed,
                    );
                }
                Ok(())
            }
            Err(n) => Err(n.into_box().into_value().unwrap()),
        }
    }

    fn delete<'a>(&'a self, key: &usize, guard: &'a Guard) -> Result<&'a V, ()> {
        Self::assert_valid_key(*key);
        let (found, cursor) = self.find(key, guard);
        if !found {
            return Err(());
        }
        match cursor.delete(guard) {
            Ok(v) => {
                self.count.fetch_sub(1, Ordering::Relaxed);
                v.as_ref().ok_or(())
            }
            Err(_) => Err(()),
        }
    }
}
