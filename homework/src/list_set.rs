use std::cmp;
use std::fmt::Debug;
use std::ptr;
use std::sync::{Mutex, MutexGuard};

#[derive(Debug)]
struct Node<T> {
    data: T,
    next: Mutex<*mut Node<T>>,
}

unsafe impl<T: Send> Send for Node<T> {}
unsafe impl<T: Sync> Sync for Node<T> {}

/// Concurrent sorted singly linked list using lock-coupling.
#[derive(Debug)]
pub struct OrderedListSet<T> {
    head: Mutex<*mut Node<T>>,
}

unsafe impl<T: Send> Send for OrderedListSet<T> {}
unsafe impl<T: Sync> Sync for OrderedListSet<T> {}

// reference to the `next` field of previous node which points to the current node
struct Cursor<'l, T>(MutexGuard<'l, *mut Node<T>>);

impl<T> Node<T> {
    fn new(data: T, next: *mut Self) -> *mut Self {
        Box::into_raw(Box::new(Self {
            data,
            next: Mutex::new(next),
        }))
    }
}

impl<'l, T: Ord> Cursor<'l, T> {
    /// Move the cursor to the position of key in the sorted list. If the key is found in the list,
    /// return `true`.
    fn find(&mut self, key: &T) -> bool {
        while let Some(node) = unsafe { (*self.0).as_ref() } {
            match node.data.cmp(key) {
                cmp::Ordering::Greater => return false,
                cmp::Ordering::Equal => return true,
                cmp::Ordering::Less => {
                    let _guard = std::mem::replace(&mut self.0, node.next.lock().unwrap());
                }
            }
        }
        false
    }
}

impl<T> OrderedListSet<T> {
    /// Creates a new list.
    pub fn new() -> Self {
        Self {
            head: Mutex::new(ptr::null_mut()),
        }
    }
}

impl<T: Ord> OrderedListSet<T>
where
    T: Debug,
{
    fn find(&self, key: &T) -> (bool, Cursor<T>) {
        let guard = self.head.lock().unwrap();
        let mut cursor = Cursor(guard);
        let result = cursor.find(key);
        (result, cursor)
    }

    /// Returns `true` if the set contains the key.
    pub fn contains(&self, key: &T) -> bool {
        self.find(key).0
    }

    /// Insert a key to the set. If the set already has the key, return the provided key in `Err`.
    pub fn insert(&self, key: T) -> Result<(), T> {
        let mut curr_guard = self.head.lock().unwrap();

        while let Some(curr_node) = unsafe { curr_guard.as_ref() } {
            match curr_node.data.cmp(&key) {
                cmp::Ordering::Less => {
                    let next_guard = curr_node.next.lock().unwrap();
                    curr_guard = next_guard;
                }
                cmp::Ordering::Equal => return Err(key),
                cmp::Ordering::Greater => {
                    let new_node = Node::new(key, *curr_guard);
                    *curr_guard = new_node;
                    return Ok(());
                }
            }
        }
        let node = Node::new(key, ptr::null_mut());
        *curr_guard = node;
        Ok(())
    }

    /// Remove the key from the set and return it.
    pub fn remove(&self, key: &T) -> Result<T, ()> {
        let mut curr_guard = self.head.lock().unwrap();
        let raw_ptr = *curr_guard;
        if raw_ptr.is_null() {
            return Err(());
        }
        while let Some(curr_node) = unsafe { (*curr_guard).as_ref() } {
            match curr_node.data.cmp(key) {
                cmp::Ordering::Less => {
                    let next_guard = curr_node.next.lock().unwrap();
                    drop(curr_guard);
                    curr_guard = next_guard;
                }
                cmp::Ordering::Equal => {
                    let removed_node = unsafe { Box::from_raw(*curr_guard) };
                    let next_guard = curr_node.next.lock().unwrap();
                    *curr_guard = *next_guard;
                    drop(curr_guard);
                    drop(next_guard);
                    return Ok(removed_node.data);
                    // return Err(());
                }
                cmp::Ordering::Greater => return Err(()),
            }
        }
        Err(())
    }
}

#[derive(Debug)]
pub struct Iter<'l, T>(Option<MutexGuard<'l, *mut Node<T>>>);

impl<T> OrderedListSet<T> {
    /// An iterator visiting all elements.
    pub fn iter(&self) -> Iter<T> {
        Iter(Some(self.head.lock().unwrap()))
    }
}

impl<'l, T> Iterator for Iter<'l, T>
where T:Debug{
    type Item = &'l T;

    fn next(&mut self) -> Option<Self::Item> {
        let guard = self.0.as_ref()?;
        let node = unsafe { guard.as_ref() }?;

        self.0 = Some(node.next.lock().unwrap());

        Some(&node.data)
    }
}

impl<T> Drop for OrderedListSet<T> {
    fn drop(&mut self) {
        let mut cursor = *self.head.lock().unwrap();
        while !cursor.is_null() {
            unsafe {
                let node = Box::from_raw(cursor);
                cursor = *node.next.lock().unwrap();
            }
        }
    }
}

impl<T> Default for OrderedListSet<T> {
    fn default() -> Self {
        Self::new()
    }
}
