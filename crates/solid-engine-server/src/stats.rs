//! Compteurs de coordination (diagnostic). Désactivés sauf si `SOLID_ENGINE_COUNTERS_FILE` est défini.
//! Chaque thread accumule localement et publie par lots : aucun atomique partagé sur le chemin chaud.

use std::io::Read;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::OnceLock;

#[derive(Clone, Copy)]
pub enum C {
    ShardCommands,
    ShardBatches,
    ShardBlockWaits,
    ShardWakeups,
    NetReads,
    NetReadBytes,
    NetWriterFrames,
    NetWriteCalls,
    NetWriteBytes,
    NetWriterBlockWaits,
    NetWriterWakeups,
    WalMsgs,
    CpuReaderNs,
    CpuShardNs,
    CpuWriterNs,
    CpuWalNs,
    CpuReadSysNs,
    CpuWriteSysNs,
}

pub const NAMES: [&str; 18] = [
    "shard_commands",
    "shard_batches",
    "shard_block_waits",
    "shard_wakeups",
    "net_reads",
    "net_read_bytes",
    "net_writer_frames",
    "net_write_calls",
    "net_write_bytes",
    "net_writer_block_waits",
    "net_writer_wakeups",
    "wal_msgs",
    "cpu_reader_ns",
    "cpu_shard_ns",
    "cpu_writer_ns",
    "cpu_wal_ns",
    "cpu_read_sys_ns",
    "cpu_write_sys_ns",
];

static TOTALS: [AtomicU64; 18] = [const { AtomicU64::new(0) }; 18];

pub fn enabled() -> bool {
    static V: OnceLock<bool> = OnceLock::new();
    *V.get_or_init(|| std::env::var_os("SOLID_ENGINE_COUNTERS_FILE").is_some())
}

pub fn snapshot() -> Vec<(&'static str, u64)> {
    NAMES.iter().zip(TOTALS.iter()).map(|(n, t)| (*n, t.load(Relaxed))).collect()
}

pub struct Tally {
    on: bool,
    v: [u64; 18],
    ticks: u32,
    cpu: Option<(C, u64)>,
}

impl Default for Tally {
    fn default() -> Self {
        Tally { on: enabled(), v: [0; 18], ticks: 0, cpu: None }
    }
}

#[repr(C)]
struct Timespec {
    tv_sec: i64,
    tv_nsec: i64,
}

extern "C" {
    fn clock_gettime(clk: i32, ts: *mut Timespec) -> i32;
}

#[cfg(target_os = "macos")]
const CLOCK_THREAD_CPUTIME_ID: i32 = 16;
#[cfg(not(target_os = "macos"))]
const CLOCK_THREAD_CPUTIME_ID: i32 = 3;

/// Temps CPU (utilisateur + noyau) consommé par le thread appelant, en ns.
pub fn thread_cpu_ns() -> u64 {
    let mut ts = Timespec { tv_sec: 0, tv_nsec: 0 };
    // SAFETY : `ts` est valide pour l'écriture pendant l'appel.
    if unsafe { clock_gettime(CLOCK_THREAD_CPUTIME_ID, &mut ts) } == 0 {
        ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
    } else {
        0
    }
}

impl Tally {
    /// Rattache ce thread à un rôle : son temps CPU sera ajouté au compteur `role` à chaque publication.
    pub fn with_cpu_role(mut self, role: C) -> Self {
        if self.on {
            self.cpu = Some((role, thread_cpu_ns()));
        }
        self
    }

    #[inline]
    pub fn is_on(&self) -> bool {
        self.on
    }

    pub fn add(&mut self, c: C, n: u64) {
        if self.on {
            self.v[c as usize] += n;
        }
    }

    /// À appeler à chaque tour de boucle : publie tous les 256 tours.
    #[inline]
    pub fn tick(&mut self) {
        if self.on {
            self.ticks += 1;
            if self.ticks & 255 == 0 {
                self.flush();
            }
        }
    }

    pub fn flush(&mut self) {
        if self.on {
            if let Some((role, last)) = self.cpu {
                let now = thread_cpu_ns();
                self.v[role as usize] += now.saturating_sub(last);
                self.cpu = Some((role, now));
            }
            for (t, v) in TOTALS.iter().zip(self.v.iter_mut()) {
                if *v != 0 {
                    t.fetch_add(*v, Relaxed);
                    *v = 0;
                }
            }
        }
    }
}

impl Drop for Tally {
    fn drop(&mut self) {
        self.flush();
    }
}

/// Compte les appels `read` (appels système) et les octets lus.
pub struct CountingReader<R> {
    pub inner: R,
    tally: Tally,
}

impl<R> CountingReader<R> {
    pub fn new(inner: R) -> Self {
        CountingReader { inner, tally: Tally::default().with_cpu_role(C::CpuReaderNs) }
    }
}

impl<R: Read> Read for CountingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let t0 = if self.tally.on { thread_cpu_ns() } else { 0 };
        let n = self.inner.read(buf)?;
        if self.tally.on {
            self.tally.add(C::CpuReadSysNs, thread_cpu_ns().saturating_sub(t0));
        }
        self.tally.add(C::NetReads, 1);
        self.tally.add(C::NetReadBytes, n as u64);
        self.tally.tick();
        Ok(n)
    }
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
#[repr(C)]
struct MallInfo2 {
    arena: usize,
    ordblks: usize,
    smblks: usize,
    hblks: usize,
    hblkhd: usize,
    usmblks: usize,
    fsmblks: usize,
    uordblks: usize,
    fordblks: usize,
    keepcost: usize,
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
extern "C" {
    fn mallinfo2() -> MallInfo2;
    fn malloc_trim(pad: usize) -> i32;
}

/// Rend à l'OS la mémoire libre de l'arène glibc, que `free` ne restitue pas seul après une grosse purge.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
pub fn trim_memory() {
    // SAFETY : appel sans pointeur, thread-safe côté glibc.
    unsafe { malloc_trim(0) };
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
pub fn trim_memory() {}

fn rss_bytes() -> usize {
    std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|s| s.split_whitespace().nth(1).and_then(|v| v.parse::<usize>().ok()))
        .map_or(0, |pages| pages * 4096)
}

/// RSS et état de l'allocateur glibc (octets) : `in_use` = alloué, `free_held` = libéré mais conservé par l'allocateur.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
pub fn process_memory() -> String {
    // SAFETY : appel sans argument, structure renvoyée par valeur.
    let m = unsafe { mallinfo2() };
    format!("rss_bytes={} malloc_in_use={} malloc_free_held={} malloc_arena={} malloc_mmapped={}", rss_bytes(), m.uordblks, m.fordblks, m.arena, m.hblkhd)
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
pub fn process_memory() -> String {
    format!("rss_bytes={}", rss_bytes())
}
