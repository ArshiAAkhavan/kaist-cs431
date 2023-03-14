//! Thread-safe key/value cache.

use std::collections::hash_map::{Entry, HashMap};
use std::collections::HashSet;
use std::default::Default;
use std::fmt::Debug;
use std::hash::Hash;
use std::rc::Rc;
use std::sync::{Arc, Condvar, Mutex, RwLock};

/// Cache that remembers the result for each key.
#[derive(Debug, Default)]
pub struct Cache<K, V> {
    data: Mutex<HashMap<K, V>>,
    preflight: Mutex<HashMap<K, Arc<Condvar>>>,
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
        {
            let data = self.data.lock().unwrap();
            if data.contains_key(&key) {
                return data.get(&key).unwrap().to_owned();
            }
        }
        let mut preflight = self.preflight.lock().unwrap();
        if preflight.contains_key(&key) {
            let condvar = Arc::clone(preflight.get(&key).unwrap());
            let guard = condvar.wait(preflight).unwrap();
        } else {
            {
                let data = self.data.lock().unwrap();
                if data.contains_key(&key) {
                    return data.get(&key).unwrap().to_owned();
                }
            }
            {
                preflight.insert(key.clone(), Default::default());
                drop(preflight);
            }
            let v = f(key.clone());
            {
                let mut data = self.data.lock().unwrap();
                data.insert(key.clone(), v.clone());
            }
            {
                let mut preflight = self.preflight.lock().unwrap();
                let condvar = preflight.remove(&key).unwrap();
                condvar.notify_all();
            }
        }
        let data = self.data.lock().unwrap();
        data.get(&key).unwrap().to_owned()
    }
}
