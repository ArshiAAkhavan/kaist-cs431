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

impl<V: Debug> Debug for SplitOrderedList<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let guard = &epoch::pin();
        let mut cursor = self.list.head(guard);
        unsafe {
            let prev =
                &*(&AtomicUsize::new(0) as *const _ as *const Atomic<Node<usize, Option<V>>>);
            while let Some(node) = cursor.curr().as_ref() {
                let key = format!("{node:#?}")
                    .lines()
                    .nth(6)
                    .unwrap()
                    .replace('\"', "")
                    .replace(',', "")
                    .replace("key:", "")
                    .trim()
                    .parse::<u64>()
                    .unwrap();
                let next = format!("{node:#?}")
                    .lines()
                    .nth(3)
                    .unwrap()
                    .replace('\"', "")
                    .replace(',', "")
                    .replace("raw:", "")
                    .trim()
                    .parse::<u64>()
                    .unwrap();
                writeln!(
                    f,
                    "{}:\t {:0>64}->",
                    ((key >> 1) << 1).reverse_bits(),
                    format_args!("{key:b}")
                )?;
                if next == 0 {
                    break;
                }
                let next = Owned::from_raw(next as *mut Node<usize, Option<V>>);
                cursor = Cursor::new(prev, next.into_shared(guard));
            }
            Ok(())
        }
    }
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
        let bucket_zero = buckets.get(1, guard);
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

        let mut bucket_raw = self.buckets.get(bucket, guard);
        let node_raw = bucket_raw.load(Ordering::Acquire, guard);
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
        let node_raw = parent_raw.load(Ordering::Acquire, guard);
        if node_raw.is_null() {
            self.make_bucket(parent, size, guard);
        }
        let cursor = self.insert_bucket(Cursor::new(parent_raw, node_raw), bucket, guard);

        let bucket = self.buckets.get(bucket, guard);
        let bucket_raw = bucket.load(Ordering::Acquire, guard);
        match bucket_raw.is_null() {
            true => bucket.store(cursor.curr(), Ordering::Release),
            false => {}
        }

        cursor
    }

    fn get_parent_bucket(&self, bucket: usize) -> usize {
        // this function panics on input 1 because of shr overflow. but since we have already
        // created bucket 1 in Self::new, this function would never be invoced with input 1
        assert_ne!(bucket, 1);
        bucket ^ (1 << (mem::size_of::<usize>() * 8 - bucket.leading_zeros() as usize - 1))
    }

    fn insert_bucket<'s>(
        &'s self,
        mut cursor: Cursor<'s, usize, Option<V>>,
        bucket: usize,
        guard: &'s Guard,
    ) -> Cursor<'s, usize, Option<V>> {
        let bucket_key = Self::get_so_bucket_key(bucket);
        // println!("{bucket_key:b}");
        let mut node = Owned::new(Node::new(bucket_key, None));
        loop {
            let found = cursor
                .find_harris_herlihy_shavit(&bucket_key, guard)
                .unwrap();
            if found {
                return cursor;
            }
            match cursor.insert(node, guard) {
                Ok(_) => break,
                Err(n) => node = n,
            }
        }
        cursor
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
    fn find<'s>(
        &'s self,
        key: &usize,
        guard: &'s Guard,
    ) -> (usize, bool, Cursor<'s, usize, Option<V>>) {
        let mut bucket_cusor = self.lookup_bucket(*key, guard);

        // println!("{:b}", Self::get_so_data_key(*key));
        let found = bucket_cusor
            .find_harris(&Self::get_so_data_key(*key), guard)
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
        if found {
            return Err(value);
        }

        let prev_count = self.count.fetch_add(1, Ordering::AcqRel) + 1;
        let prev_size = self.size.load(Ordering::Acquire);
        if prev_count > prev_size * Self::LOAD_FACTOR {
            // we don't care about the results, both way, we win!
            let _ = self.size.compare_exchange(
                prev_size,
                prev_size * 2,
                Ordering::Release,
                Ordering::Relaxed,
            );
        }
        let (size, found, mut cursor) = self.find(key, guard);
        // println!("{:b}", Self::get_so_data_key(*key));
        let mut node = Owned::new(Node::new(Self::get_so_data_key(*key), Some(value)));
        loop {
            match cursor.insert(node, guard) {
                Ok(_) => {
                    break;
                }
                Err(n) => node = n,
            }
        }
        Ok(())
    }

    fn delete<'a>(&'a self, key: &usize, guard: &'a Guard) -> Result<&'a V, ()> {
        Self::assert_valid_key(*key);
        loop {
            let (size, found, cursor) = self.find(key, guard);
            if !found {
                return Err(());
            }
            if let Ok(v) = cursor.delete(guard) {
                self.count.fetch_sub(1, Ordering::AcqRel);
                return v.as_ref().ok_or(());
            }
        }
    }
}
