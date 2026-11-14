use solid_engine_core::{PerShard, MAX_SHARDS};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::*};
use std::sync::{Barrier, Mutex};

const MAGIC: u64 = 0x5EED_CAFE_F00D_0001;
const THREADS: usize = 32;

static LIVE: AtomicUsize = AtomicUsize::new(0);
// Les tests partagent LIVE : on les sérialise.
static SERIAL: Mutex<()> = Mutex::new(());

struct Cell {
    magic: u64,
    n: AtomicU64,
}

impl Default for Cell {
    fn default() -> Self {
        LIVE.fetch_add(1, SeqCst);
        std::thread::yield_now(); // élargit la fenêtre de course
        Cell { magic: MAGIC, n: AtomicU64::new(0) }
    }
}

impl Drop for Cell {
    fn drop(&mut self) {
        LIVE.fetch_sub(1, SeqCst);
    }
}

#[test]
fn same_slot_race_publishes_one_instance() {
    let _g = SERIAL.lock().unwrap();
    let live0 = LIVE.load(SeqCst);
    let table: Box<PerShard<Cell>> = Box::new(PerShard::new());
    let barrier = Barrier::new(THREADS);
    let addrs: Vec<usize> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..THREADS)
            .map(|_| {
                s.spawn(|| {
                    barrier.wait();
                    let c = table.get(7);
                    assert_eq!(c.magic, MAGIC);
                    for _ in 0..1000 {
                        c.n.fetch_add(1, Relaxed);
                    }
                    c as *const Cell as usize
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    assert!(addrs.iter().all(|a| *a == addrs[0]), "plusieurs instances publiées");
    assert_eq!(table.get(7).n.load(SeqCst), THREADS as u64 * 1000);
    // Les allocations perdantes ont été libérées : une seule instance vit encore.
    assert_eq!(LIVE.load(SeqCst) - live0, 1);
    assert!(table.peek(8).is_none());
    drop(table);
    assert_eq!(LIVE.load(SeqCst), live0, "fuite après drop de la table");
}

#[test]
fn distinct_slots_get_one_instance_each() {
    let _g = SERIAL.lock().unwrap();
    let live0 = LIVE.load(SeqCst);
    let table: Box<PerShard<Cell>> = Box::new(PerShard::new());
    let barrier = Barrier::new(THREADS);
    std::thread::scope(|s| {
        for t in 0..THREADS {
            let (table, barrier) = (&table, &barrier);
            s.spawn(move || {
                barrier.wait();
                for k in 0..64 {
                    let c = table.get((k + t) % 64);
                    assert_eq!(c.magic, MAGIC);
                    c.n.fetch_add(1, Relaxed);
                }
            });
        }
    });
    for i in 0..64 {
        assert_eq!(table.peek(i).unwrap().n.load(SeqCst), THREADS as u64, "entrée {i}");
    }
    assert_eq!(LIVE.load(SeqCst) - live0, 64);
    drop(table);
    assert_eq!(LIVE.load(SeqCst), live0, "fuite après drop de la table");
}

#[test]
fn out_of_range_index() {
    let table: Box<PerShard<AtomicU64>> = Box::new(PerShard::new());
    assert!(table.peek(MAX_SHARDS).is_none());
    assert!(table.peek(usize::MAX).is_none());
    table.get(MAX_SHARDS - 1).fetch_add(1, SeqCst);
    assert_eq!(table.peek(MAX_SHARDS - 1).unwrap().load(SeqCst), 1);
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        table.get(MAX_SHARDS);
    }));
    assert!(r.is_err());
}
