use super::*;
use solid_engine_core::*;

struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        let p = std::env::temp_dir().join(format!("solid-engine-{tag}-{}-{}", std::process::id(), now_ms()));
        let _ = fs::remove_dir_all(&p);
        fs::create_dir_all(&p).unwrap();
        Self(p)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn cfg() -> Arc<Config> {
    let mut c = Config::default();
    c.jobs.retry.jitter = 0.0;
    Arc::new(c)
}

fn push(id: u128) -> PushRequest {
    PushRequest {
        job_id: id,
        channel_id: 1,
        priority: 0,
        max_attempts: 0,
        delay_ms: 0,
        partition_key: vec![],
        payload: id.to_le_bytes().to_vec(),
    }
}

fn persist(shard: &mut Shard, wal: &mut Wal) {
    wal.append(&shard.take_log()).unwrap();
    wal.sync().unwrap();
}

fn segment_path(dir: &Path) -> PathBuf {
    list(dir, "wal-", ".log").unwrap().pop().unwrap().1
}

fn reservation(c: &ClaimedJob) -> Reservation {
    Reservation { job_id: c.job_id, reservation_id: c.reservation_id, fencing_token: c.fencing_token }
}

#[test]
fn replay_restores_jobs_and_invalidates_old_reservations() {
    let t = TempDir::new("replay");
    let (mut shard, mut wal) = recover_shard(&t.0, 0, cfg()).unwrap();
    for id in 1..=3 {
        shard.push(push(id), now_ms()).unwrap();
    }
    let old = shard.claim(1, 60_000, now_ms()).unwrap().unwrap();
    let done = shard.claim(1, 60_000, now_ms()).unwrap().unwrap();
    shard.ack(reservation(&done), now_ms()).unwrap();
    persist(&mut shard, &mut wal);
    drop((shard, wal)); // crash : rien d'autre n'est écrit

    // Redémarrages répétés : l'état reste stable.
    for _ in 0..3 {
        let (mut shard, _wal) = recover_shard(&t.0, 0, cfg()).unwrap();
        let stats = shard.stats(Some(1), now_ms());
        assert_eq!((stats.ready, stats.claimed, stats.completed), (2, 0, 1));
        assert_eq!(shard.get(done.job_id, now_ms()).unwrap().state, JobState::Completed);
    }

    let (mut shard, mut wal) = recover_shard(&t.0, 0, cfg()).unwrap();
    // L'ancien worker ne peut plus acquitter ; la nouvelle réservation a un token supérieur.
    assert_eq!(shard.ack(reservation(&old), now_ms()).unwrap_err().code, ErrorCode::StaleReservation);
    let mut seen = Vec::new();
    while let Some(c) = shard.claim(1, 60_000, now_ms()).unwrap() {
        if c.job_id == old.job_id {
            assert!(c.fencing_token > old.fencing_token);
        }
        seen.push(c.job_id);
    }
    seen.sort();
    assert_eq!(seen, vec![1, 3]);
    persist(&mut shard, &mut wal);
}

#[test]
fn torn_tail_is_truncated_and_log_stays_appendable() {
    let t = TempDir::new("torn");
    let (mut shard, mut wal) = recover_shard(&t.0, 0, cfg()).unwrap();
    shard.push(push(1), now_ms()).unwrap();
    shard.push(push(2), now_ms()).unwrap();
    persist(&mut shard, &mut wal);
    drop((shard, wal));

    let seg = segment_path(&t.0);
    let len = fs::metadata(&seg).unwrap().len();
    OpenOptions::new().write(true).open(&seg).unwrap().set_len(len - 5).unwrap();

    let (mut shard, mut wal) = recover_shard(&t.0, 0, cfg()).unwrap();
    assert_eq!(shard.stats(Some(1), now_ms()).ready, 1);
    shard.push(push(3), now_ms()).unwrap();
    persist(&mut shard, &mut wal);
    drop((shard, wal));
    let (mut shard, _) = recover_shard(&t.0, 0, cfg()).unwrap();
    assert_eq!(shard.stats(Some(1), now_ms()).ready, 2);
}

#[test]
fn mid_log_corruption_is_an_explicit_error() {
    let t = TempDir::new("corrupt");
    let (mut shard, mut wal) = recover_shard(&t.0, 0, cfg()).unwrap();
    for id in 1..=3 {
        shard.push(push(id), now_ms()).unwrap();
    }
    persist(&mut shard, &mut wal);
    drop((shard, wal));

    let seg = segment_path(&t.0);
    let mut data = fs::read(&seg).unwrap();
    data[SEGMENT_MAGIC.len() + RECORD_HEADER_LEN + 2] ^= 0xFF; // payload du premier enregistrement
    fs::write(&seg, data).unwrap();
    assert!(matches!(recover_shard(&t.0, 0, cfg()), Err(StorageError::Corrupt(_))));
}

#[test]
fn snapshot_truncates_wal_and_recovery_combines_both() {
    let t = TempDir::new("snapshot");
    let (mut shard, mut wal) = recover_shard(&t.0, 0, cfg()).unwrap();
    for id in 1..=5 {
        shard.push(push(id), now_ms()).unwrap();
    }
    let c = shard.claim(1, 60_000, now_ms()).unwrap().unwrap();
    persist(&mut shard, &mut wal);
    wal.snapshot(&shard.encode_snapshot()).unwrap();
    assert_eq!(wal.records_since_snapshot(), 0);
    assert_eq!(list(&t.0, "wal-", ".log").unwrap().len(), 1);

    shard.ack(reservation(&c), now_ms()).unwrap();
    shard.push(push(6), now_ms()).unwrap();
    persist(&mut shard, &mut wal);
    drop((shard, wal));

    let (mut shard, mut wal) = recover_shard(&t.0, 0, cfg()).unwrap();
    let s = shard.stats(Some(1), now_ms());
    assert_eq!((s.ready, s.completed), (5, 1));
    // FIFO préservé à travers snapshot + WAL.
    assert_eq!(shard.claim(1, 1000, now_ms()).unwrap().unwrap().job_id, 2);
    persist(&mut shard, &mut wal);
}

#[test]
fn corrupt_snapshot_without_wal_history_is_an_error() {
    let t = TempDir::new("badsnap");
    let (mut shard, mut wal) = recover_shard(&t.0, 0, cfg()).unwrap();
    shard.push(push(1), now_ms()).unwrap();
    persist(&mut shard, &mut wal);
    wal.snapshot(&shard.encode_snapshot()).unwrap();
    drop((shard, wal));

    let snap = list(&t.0, "snap-", ".snap").unwrap().pop().unwrap().1;
    let mut data = fs::read(&snap).unwrap();
    let mid = data.len() / 2;
    data[mid] ^= 0xFF;
    fs::write(&snap, data).unwrap();
    assert!(matches!(recover_shard(&t.0, 0, cfg()), Err(StorageError::Corrupt(_))));
}

#[test]
fn incomplete_snapshot_file_is_ignored() {
    let t = TempDir::new("tmpsnap");
    let (mut shard, mut wal) = recover_shard(&t.0, 0, cfg()).unwrap();
    shard.push(push(1), now_ms()).unwrap();
    persist(&mut shard, &mut wal);
    drop((shard, wal));
    fs::write(t.0.join("snap-00000000000000000001.snap.tmp"), b"partial").unwrap();
    let (mut shard, _) = recover_shard(&t.0, 0, cfg()).unwrap();
    assert_eq!(shard.stats(Some(1), now_ms()).ready, 1);
}

#[test]
fn data_dir_is_locked_and_pinned_to_shard_count() {
    let t = TempDir::new("lock");
    let lock = lock_data_dir(&t.0, 4).unwrap();
    assert!(lock_data_dir(&t.0, 4).is_err());
    drop(lock);
    assert!(matches!(lock_data_dir(&t.0, 8), Err(StorageError::Corrupt(_))));
    lock_data_dir(&t.0, 4).unwrap();
}
