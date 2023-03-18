use std::cmp;
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

impl<T: Ord> OrderedListSet<T> {
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
        let mut prev: Option<Cursor<T>> = None;
        let guard = self.head.lock().unwrap();
        let mut curr = Cursor(guard);
        while let Some(node) = unsafe { (*curr.0).as_ref() } {
            match node.data.cmp(&key) {
                cmp::Ordering::Greater => {
                    let node = Node::new(key, *curr.0);
                    match prev {
                        Some(prev_cursor) => {
                            let prev_node = unsafe { &*(prev_cursor.0).as_mut().unwrap() };
                            *prev_node.next.lock().unwrap() = node;
                        }
                        None => *curr.0 = node,
                    }
                    return Ok(());
                }
                cmp::Ordering::Equal => return Err(key),
                cmp::Ordering::Less => {
                    let prev_guard = std::mem::replace(&mut curr.0, node.next.lock().unwrap());
                    prev = Some(Cursor(prev_guard));
                }
            }
        }
        // head in null
        let node = Node::new(key, ptr::null_mut());
        let mut guard = curr.0;
        *guard = node;
        Ok(())
    }

    /// Remove the key from the set and return it.
    pub fn remove(&self, key: &T) -> Result<T, ()> {
        let mut prev: Option<Cursor<T>> = None;
        let guard = self.head.lock().unwrap();
        let mut curr = Cursor(guard);
        while let Some(node) = unsafe { (*curr.0).as_ref() } {
            match node.data.cmp(key) {
                cmp::Ordering::Greater => return Err(()),
                cmp::Ordering::Equal => {
                    let data = *(curr.0);
                    match prev {
                        Some(prev_cursor) => {
                            let prev_node = unsafe { (*prev_cursor.0).as_ref() }.unwrap();
                            *prev_node.next.lock().unwrap() = *curr.0;
                        }
                        None => {
                            let next = unsafe { (*curr.0).as_ref() }.unwrap();
                            *curr.0 = *next.next.lock().unwrap();
                        }
                    }
                    return Err(());
                    // return Ok(unsafe { (*data) }.data);
                }
                cmp::Ordering::Less => {
                    let prev_guard = std::mem::replace(&mut curr.0, node.next.lock().unwrap());
                    prev = Some(Cursor(prev_guard));
                }
            }
        }
        // head in null
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

impl<'l, T> Iterator for Iter<'l, T> {
    type Item = &'l T;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

impl<T> Drop for OrderedListSet<T> {
    fn drop(&mut self) {
        todo!()
    }
}

impl<T> Default for OrderedListSet<T> {
    fn default() -> Self {
        Self::new()
    }
}
