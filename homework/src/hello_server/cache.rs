//! Thread-safe key/value cache.

use std::collections::hash_map::{Entry, HashMap};
use std::collections::HashSet;
use std::default::Default;
use std::fmt::Debug;
use std::hash::Hash;
use std::rc::Rc;
use std::sync::{Arc, Condvar, Mutex, RwLock};

#[derive(Debug)]
enum CacheEntry<V> {
    Value(V),
    Computing(Arc<Condvar>),
}

impl<V> Default for CacheEntry<V> {
    fn default() -> Self {
        Self::Computing(Arc::new(Condvar::new()))
    }
}

/// Cache that remembers the result for each key.
#[derive(Debug, Default)]
pub struct Cache<K, V> {
    data: Mutex<HashMap<K, CacheEntry<V>>>,
}

impl<K: Eq + Hash + Clone, V: Clone> Cache<K, V> {
    /// Retrieve the value or insert a new one created by `f`.
    ///
    /// An invocation to this function should not block another invocation with a different key.
    /// For example, if a thread calls `get_or_insert_with(key1, f1)` and another thread calls
    /// `get_or_insert_with(key2, f2)` (`key1≠key2`, `key1,key2∉cache`) concurrently, `f1` and `f2`
    /// should run concurrently.
    ///
    /// On the other hand, since `f` may consume a lot of resource (= money), it's desirable not to
    /// duplicate the work. That is, `f` should be run only once for each key. Specifically, even
    /// for the concurrent invocations of `get_or_insert_with(key, f)`, `f` is called only once.
    ///
    /// Hint: the [`Entry`] API may be useful in implementing this function.
    ///
    /// [`Entry`]: https://doc.rust-lang.org/stable/std/collections/hash_map/struct.HashMap.html#method.entry
    pub fn get_or_insert_with<F: FnOnce(K) -> V>(&self, key: K, f: F) -> V {
        let mut data = self.data.lock().unwrap();
        if let Some(entry) = data.get(&key) {
            // there has been previouse attempts to fetch this key
            match entry {
                CacheEntry::Value(v) => v.to_owned(),
                CacheEntry::Computing(c) => {
                    let data = Arc::clone(c).wait(data).unwrap();
                    let v = data.get(&key).unwrap();
                    match v {
                        CacheEntry::Value(v) => v.to_owned(),
                        CacheEntry::Computing(_) => unreachable!(),
                    }
                }
            }
        } else {
            // first one to ever fetch the key
            data.insert(key.clone(), Default::default());
            drop(data);
            let v = f(key.clone());
            let mut data = self.data.lock().unwrap();
            let condvar = data.remove(&key).unwrap();
            data.insert(key, CacheEntry::Value(v.clone()));
            if let CacheEntry::Computing(condvar) = condvar {
                condvar.notify_all();
            }
            v
        }
    }
}
