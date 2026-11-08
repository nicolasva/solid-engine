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
fn record_wire_len_matches_bytes_written() {
    let t = TempDir::new("wirelen");
    let (mut shard, mut wal) = recover_shard(&t.0, 0, cfg()).unwrap();
    shard.push(push(1), now_ms()).unwrap();
    shard.push(push(2), now_ms()).unwrap();
    let c = shard.claim(1, 60_000, now_ms()).unwrap().unwrap();
    shard.ack(reservation(&c), now_ms()).unwrap();
    let mut records = shard.take_log();
    records.push(Record::Retry { job_id: 9, at: 1, scheduled_at: 2 });
    records.push(Record::Extend { job_id: 9, lease_deadline: 3 });
    records.push(Record::Dead { job_id: 9, at: 4 });
    records.push(Record::Cancel { job_id: 9, at: 5 });
    let before = fs::metadata(segment_path(&t.0)).unwrap().len();
    wal.append(&records).unwrap();
    wal.sync().unwrap();
    let after = fs::metadata(segment_path(&t.0)).unwrap().len();
    assert_eq!(after - before, records.iter().map(|r| r.wire_len()).sum::<u64>());
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

fn copy_files(from: &Path, to: &Path) {
    for e in fs::read_dir(from).unwrap() {
        let e = e.unwrap();
        if e.file_type().unwrap().is_file() && e.file_name() != "LOCK" {
            fs::copy(e.path(), to.join(e.file_name())).unwrap();
        }
    }
}

// Un arrêt brutal peut survenir entre les étapes de `Wal::snapshot` : publication du snapshot, création du
// nouveau segment, suppression de l'ancien. Chaque état intermédiaire doit se récupérer sans perte ni doublon.
#[test]
fn crash_between_snapshot_steps_recovers_exactly() {
    let t = TempDir::new("snapcrash");
    let (mut shard, mut wal) = recover_shard(&t.0, 0, cfg()).unwrap();
    for id in 1..=5 {
        shard.push(push(id), now_ms()).unwrap();
    }
    let c = shard.claim(1, 60_000, now_ms()).unwrap().unwrap();
    persist(&mut shard, &mut wal);
    let before = TempDir::new("snapcrash-before");
    copy_files(&t.0, &before.0);
    wal.snapshot(&shard.encode_snapshot()).unwrap();
    shard.ack(reservation(&c), now_ms()).unwrap();
    persist(&mut shard, &mut wal);
    drop((shard, wal));
    let old_segment = list(&before.0, "wal-", ".log").unwrap().pop().unwrap();
    let snap_seq = list(&t.0, "snap-", ".snap").unwrap().pop().unwrap().0;

    // 1. snapshot publié, ancien segment encore présent, nouveau segment absent.
    // 2. snapshot publié, ancien et nouveau segments présents.
    for (tag, keep_new) in [("published", false), ("rotated", true)] {
        let d = TempDir::new(tag);
        fs::copy(list(&t.0, "snap-", ".snap").unwrap().pop().unwrap().1, d.0.join(snapshot_name(snap_seq))).unwrap();
        fs::copy(&old_segment.1, d.0.join(segment_name(old_segment.0))).unwrap();
        let new_segment = list(&t.0, "wal-", ".log").unwrap().pop().unwrap();
        if keep_new {
            fs::copy(&new_segment.1, d.0.join(segment_name(new_segment.0))).unwrap();
        }
        let (mut shard, mut wal) = recover_shard(&d.0, 0, cfg()).unwrap();
        // Le nouveau segment contient l'ACK posté après le snapshot.
        let ready = if keep_new { 4 } else { 5 };
        assert_eq!(shard.stats(Some(1), now_ms()).ready, ready, "{tag}");
        shard.push(push(9), now_ms()).unwrap();
        persist(&mut shard, &mut wal);
        drop((shard, wal));
        let (mut shard, _) = recover_shard(&d.0, 0, cfg()).unwrap();
        assert_eq!(shard.stats(Some(1), now_ms()).ready, ready + 1, "{tag}");
    }
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

#[test]
fn snapshot_file_layout_is_header_state_crc() {
    let t = TempDir::new("snap-layout");
    let (mut shard, mut wal) = recover_shard(&t.0, 3, cfg()).unwrap();
    for id in 1..=4 {
        shard.push(push(id), now_ms()).unwrap();
    }
    persist(&mut shard, &mut wal);
    let state = shard.encode_snapshot();
    wal.snapshot(&state).unwrap();
    let (seq, path) = list(&t.0, "snap-", ".snap").unwrap().pop().unwrap();
    let data = fs::read(path).unwrap();
    let mut expect = Vec::new();
    expect.extend_from_slice(SNAPSHOT_MAGIC);
    expect.extend_from_slice(&3u16.to_le_bytes());
    expect.extend_from_slice(&seq.to_le_bytes());
    expect.extend_from_slice(&(state.len() as u64).to_le_bytes());
    expect.extend_from_slice(&state);
    let crc = crc32fast::hash(&expect);
    expect.extend_from_slice(&crc.to_le_bytes());
    assert_eq!(data, expect);
}
