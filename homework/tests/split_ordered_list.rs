use crossbeam_epoch as epoch;
use cs431_homework::{NonblockingConcurrentMap, NonblockingMap, SplitOrderedList};

pub mod map;

#[test]
pub fn smoke() {
    let list = SplitOrderedList::<usize>::new();

    let guard = epoch::pin();

    assert_eq!(list.insert(&37, 37, &guard), Ok(()));
    assert_eq!(list.lookup(&42, &guard), None);
    assert_eq!(list.lookup(&37, &guard), Some(&37));

    assert_eq!(list.insert(&42, 42, &guard), Ok(()));
    assert_eq!(list.lookup(&42, &guard), Some(&42));
    assert_eq!(list.lookup(&37, &guard), Some(&37));

    assert_eq!(list.delete(&37, &guard), Ok(&37));
    assert_eq!(list.lookup(&42, &guard), Some(&42));
    assert_eq!(list.lookup(&37, &guard), None);

    assert_eq!(list.delete(&37, &guard), Err(()));
    assert_eq!(list.lookup(&42, &guard), Some(&42));
    assert_eq!(list.lookup(&37, &guard), None);
}

#[test]
pub fn fire() {
    let list = SplitOrderedList::<usize>::new();
    println!("{list:?}");

    let guard = epoch::pin();

    assert_eq!(list.insert(&9, 9, &guard), Ok(()));
    println!("{list:?}");
    assert_eq!(list.insert(&8, 8, &guard), Ok(()));
    println!("{list:?}");
    assert_eq!(list.insert(&13, 13, &guard), Ok(()));
    println!("{list:?}");
    assert_eq!(list.insert(&5, 5, &guard), Ok(()));
    println!("{list:?}");
    assert_eq!(list.insert(&7, 7, &guard), Ok(()));
    println!("{list:?}");
    assert_eq!(list.lookup(&7, &guard), Some(&7));
    println!("{list:?}");
}

#[test]
pub fn buckets() {
    let list = SplitOrderedList::<usize>::new();
    println!("{list:?}");

    let guard = epoch::pin();
    assert_eq!(list.insert(&0, 0, &guard), Ok(()));
    assert_eq!(list.insert(&1, 1, &guard), Ok(()));
    assert_eq!(list.insert(&2, 2, &guard), Ok(()));
    assert_eq!(list.insert(&3, 3, &guard), Ok(()));
    assert_eq!(list.insert(&4, 4, &guard), Ok(()));
    assert_eq!(list.insert(&6, 6, &guard), Ok(()));
    assert_eq!(list.insert(&8, 8, &guard), Ok(()));
    println!("{list:?}");
    assert_eq!(list.insert(&10, 10, &guard), Ok(()));
    assert_eq!(list.insert(&14, 14, &guard), Ok(()));
    // list.lookup_bucket(6, &guard);
    println!("{list:?}");
    // list.lookup_bucket(5, &guard);
    println!("{list:?}");
    // list.lookup_bucket(4, &guard);
    println!("{list:?}");
    // assert_eq!(list.insert(&11, 11, &guard), Ok(()));
    // list.lookup_bucket(3, &guard);
    assert_eq!(list.insert(&7, 7, &guard), Ok(()));
    // list.lookup_bucket(7, &guard);
    println!("{list:?}");
}

#[test]
fn stress_sequential() {
    const STEPS: usize = 4096;
    map::stress_concurrent_sequential::<
        usize,
        NonblockingConcurrentMap<_, _, SplitOrderedList<usize>>,
    >(STEPS);
}

#[test]
fn lookup_concurrent() {
    const THREADS: usize = 4;
    const STEPS: usize = 4096;
    map::lookup_concurrent::<usize, NonblockingConcurrentMap<_, _, SplitOrderedList<usize>>>(
        THREADS, STEPS,
    );
}

#[test]
fn insert_concurrent() {
    const THREADS: usize = 8;
    const STEPS: usize = 4096 * 4;
    map::insert_concurrent::<usize, NonblockingConcurrentMap<_, _, SplitOrderedList<usize>>>(
        THREADS, STEPS,
    );
}

#[test]
fn stress_concurrent() {
    const THREADS: usize = 16;
    const STEPS: usize = 4096 * 512;
    map::stress_concurrent::<usize, NonblockingConcurrentMap<_, _, SplitOrderedList<usize>>>(
        THREADS, STEPS,
    );
}

#[test]
fn log_concurrent() {
    const THREADS: usize = 16;
    const STEPS: usize = 4096 * 64;
    map::log_concurrent::<usize, NonblockingConcurrentMap<_, _, SplitOrderedList<usize>>>(
        THREADS, STEPS,
    );
}
