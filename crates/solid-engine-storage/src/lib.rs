//! Persistance d'un shard : WAL segmenté, snapshots atomiques et récupération après crash.
//!
//! Disposition d'un répertoire de shard :
//! - `wal-<premier_seq>.log` : segments (en-tête `SEWAL`, puis enregistrements) ;
//! - `snap-<dernier_seq>.snap` : snapshots (checksum CRC32 final), écrits via fichier temporaire + rename.
//!
//! Format d'un enregistrement (little-endian) :
//! `payload_length u32 | record_version u8 | record_type u8 | shard_id u16 | sequence u64 | job_id u128 | payload | crc32 u32`
//! (le CRC couvre l'en-tête et le payload).
//!
//! Règles de récupération :
//! - un enregistrement incomplet ou dont le CRC est faux **et qui termine le dernier segment** est une
//!   écriture déchirée : il est tronqué (il n'a jamais été confirmé en mode strict) ;
//! - toute autre anomalie (CRC faux suivi de données, trou de séquence, segment ancien corrompu,
//!   snapshot invalide sans alternative) est une erreur explicite `Corrupt`.

use solid_engine_core::config::Config;
use solid_engine_core::record::{Record, RECORD_VERSION};
use solid_engine_core::shard::Shard;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

const SEGMENT_MAGIC: &[u8; 8] = b"SEWAL\0\0\x01";
const SNAPSHOT_MAGIC: &[u8; 8] = b"SESNAP\0\x02";
const RECORD_HEADER_LEN: usize = 32;
const CRC_LEN: usize = 4;
const FORMAT_VERSION: u32 = 1;

#[derive(Debug)]
pub enum StorageError {
    Io(io::Error),
    /// Corruption irrécupérable : jamais de récupération silencieuse avec perte de jobs.
    Corrupt(String),
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "storage I/O error: {e}"),
            Self::Corrupt(m) => write!(f, "storage corruption: {m}"),
        }
    }
}

impl std::error::Error for StorageError {}

impl From<io::Error> for StorageError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

fn corrupt<T>(msg: impl Into<String>) -> Result<T, StorageError> {
    Err(StorageError::Corrupt(msg.into()))
}

/// Verrou exclusif du répertoire de données + vérification du nombre de shards.
pub struct DataDirLock {
    _file: File,
}

pub fn lock_data_dir(dir: &Path, shard_count: usize) -> Result<DataDirLock, StorageError> {
    fs::create_dir_all(dir)?;
    let file = OpenOptions::new().create(true).truncate(false).write(true).open(dir.join("LOCK"))?;
    file.try_lock().map_err(|_| {
        StorageError::Io(io::Error::new(io::ErrorKind::WouldBlock, "data directory is locked by another process"))
    })?;
    let meta = dir.join("META");
    let expected = format!("format={FORMAT_VERSION}\nshards={shard_count}\n");
    match fs::read_to_string(&meta) {
        Ok(found) if found == expected => {}
        Ok(found) => {
            return corrupt(format!(
                "META mismatch (found {:?}, expected {:?}): changing the shard count requires a migration",
                found.replace('\n', " "),
                expected.replace('\n', " ")
            ))
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            fs::write(&meta, expected)?;
            sync_dir(dir)?;
        }
        Err(e) => return Err(e.into()),
    }
    Ok(DataDirLock { _file: file })
}

fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

fn segment_name(first_seq: u64) -> String {
    format!("wal-{first_seq:020}.log")
}

fn snapshot_name(seq: u64) -> String {
    format!("snap-{seq:020}.snap")
}

fn list(dir: &Path, prefix: &str, suffix: &str) -> io::Result<Vec<(u64, PathBuf)>> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(num) = name.strip_prefix(prefix).and_then(|n| n.strip_suffix(suffix)) {
            if let Ok(n) = num.parse::<u64>() {
                out.push((n, entry.path()));
            }
        }
    }
    out.sort();
    Ok(out)
}

/// Injection de panne pour les tests : tant qu'il est à `true`, toute écriture ou fsync du WAL échoue.
#[doc(hidden)]
pub static INJECT_IO_ERROR: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Tests : bitmask des shards dont le fsync est retenu tant que le bit reste levé.
#[doc(hidden)]
pub static HOLD_SYNC: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// Tests : bitmask des shards dont le writer est arrêté sur le point de blocage.
#[doc(hidden)]
pub static SYNC_BLOCKED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Tests : délai (ms) ajouté à chaque écriture de snapshot, pour simuler un disque lent.
#[doc(hidden)]
pub static SNAPSHOT_DELAY_MS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn hold_sync(shard_id: u16) {
    use std::sync::atomic::Ordering::SeqCst;
    let bit = 1u64 << shard_id;
    if HOLD_SYNC.load(SeqCst) & bit == 0 {
        return;
    }
    SYNC_BLOCKED.fetch_or(bit, SeqCst);
    while HOLD_SYNC.load(SeqCst) & bit != 0 {
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    SYNC_BLOCKED.fetch_and(!bit, SeqCst);
}

fn injected() -> io::Result<()> {
    if INJECT_IO_ERROR.load(std::sync::atomic::Ordering::SeqCst) {
        return Err(io::Error::other("injected I/O error"));
    }
    Ok(())
}

/// Tests (camp35-E, chemin `snapshot_begin`/`publish`/`snapshot_finish`) : frontière (1..=6) où une erreur d'I/O est injectée.
/// Compilé uniquement avec la feature `snapshot-fault-injection`.
#[cfg(feature = "snapshot-fault-injection")]
#[doc(hidden)]
pub static SNAPSHOT_FAIL_AT: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);

/// Frontières de publication d'un snapshot : 1 avant création du .tmp, 2 après écriture, 3 après sync_all,
/// 4 après rename, 5 avant rotation du segment, 6 après rotation et avant nettoyage.
/// Avec la feature `snapshot-fault-injection`, `SOLID_ENGINE_TEST_SNAPSHOT_CRASH_AT=n` arrête brutalement le
/// processus (équivalent d'un SIGKILL) à la frontière n. Sans la feature, c'est un no-op : aucune variable
/// d'environnement n'est lue.
#[cfg(feature = "snapshot-fault-injection")]
fn boundary(n: u8) -> io::Result<()> {
    static CRASH: std::sync::OnceLock<u8> = std::sync::OnceLock::new();
    let crash = *CRASH.get_or_init(|| std::env::var("SOLID_ENGINE_TEST_SNAPSHOT_CRASH_AT").ok().and_then(|v| v.parse().ok()).unwrap_or(0));
    if crash == n {
        std::process::abort();
    }
    if SNAPSHOT_FAIL_AT.load(std::sync::atomic::Ordering::SeqCst) == n {
        return Err(io::Error::other(format!("injected snapshot failure at boundary {n}")));
    }
    Ok(())
}

#[cfg(not(feature = "snapshot-fault-injection"))]
#[inline(always)]
fn boundary(_n: u8) -> io::Result<()> {
    Ok(())
}

/// Délai de snapshot (ms) imposé par `SOLID_ENGINE_TEST_SNAPSHOT_DELAY_MS` (feature `snapshot-fault-injection`).
#[cfg(feature = "snapshot-fault-injection")]
fn env_snapshot_delay_ms() -> u64 {
    static ENV_DELAY: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *ENV_DELAY.get_or_init(|| std::env::var("SOLID_ENGINE_TEST_SNAPSHOT_DELAY_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(0))
}

#[cfg(not(feature = "snapshot-fault-injection"))]
#[inline(always)]
fn env_snapshot_delay_ms() -> u64 {
    0
}

/// Publication d'un snapshot hors du thread du writer (camp35-E). Le writer garde la frontière WAL (`seq`),
/// la rotation et la suppression des segments ; le worker ne touche qu'au fichier de snapshot.
pub struct SnapshotJob {
    dir: PathBuf,
    shard_id: u16,
    pub seq: u64,
}

impl SnapshotJob {
    /// Écrit `snap-{seq}.snap` de façon atomique (tmp, sync_all, rename, sync du répertoire).
    pub fn publish(&self, state: &[u8]) -> io::Result<()> {
        let delay = SNAPSHOT_DELAY_MS.load(std::sync::atomic::Ordering::Relaxed).max(env_snapshot_delay_ms());
        if delay > 0 {
            std::thread::sleep(std::time::Duration::from_millis(delay));
        }
        boundary(1)?;
        let mut head = Vec::with_capacity(32);
        head.extend_from_slice(SNAPSHOT_MAGIC);
        head.extend_from_slice(&self.shard_id.to_le_bytes());
        head.extend_from_slice(&self.seq.to_le_bytes());
        head.extend_from_slice(&(state.len() as u64).to_le_bytes());
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&head);
        hasher.update(state);
        let crc = hasher.finalize();
        let tmp = self.dir.join(format!("{}.tmp", snapshot_name(self.seq)));
        let mut f = File::create(&tmp)?;
        f.write_all(&head)?;
        f.write_all(state)?;
        f.write_all(&crc.to_le_bytes())?;
        boundary(2)?;
        f.sync_all()?;
        boundary(3)?;
        fs::rename(&tmp, self.dir.join(snapshot_name(self.seq)))?;
        boundary(4)?;
        sync_dir(&self.dir)
    }
}

pub struct Wal {
    dir: PathBuf,
    shard_id: u16,
    file: File,
    segment_first_seq: u64,
    next_seq: u64,
    snapshot_seq: u64,
    since_snapshot: u64,
    unsynced: bool,
}

/// Diagnostic du WAL (camp35). Compilé uniquement avec la feature `wal-diagnostics` ; sans elle, les minuteurs
/// ci-dessous sont des types vides dont les méthodes sont des no-op (aucun coût sur le chemin de production).
#[cfg(feature = "wal-diagnostics")]
mod diag {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
    use std::time::Instant;

    /// Activé par le serveur quand `SOLID_ENGINE_WAL_LAG` est défini.
    pub static DIAG_ON: AtomicBool = AtomicBool::new(false);
    /// Temps d'encodage et de `write_all` par shard, cumulés en ns.
    #[derive(Default)]
    struct EncodeWriteNs {
        encode: AtomicU64,
        write: AtomicU64,
    }
    static ENCODE_WRITE_NS: solid_engine_core::PerShard<EncodeWriteNs> = solid_engine_core::PerShard::new();
    pub fn diag_encode_ns(shard: usize) -> u64 {
        ENCODE_WRITE_NS.peek(shard).map_or(0, |c| c.encode.load(Relaxed))
    }
    pub fn diag_write_ns(shard: usize) -> u64 {
        ENCODE_WRITE_NS.peek(shard).map_or(0, |c| c.write.load(Relaxed))
    }
    /// Phases d'un snapshot (µs) : sync WAL préalable, création+écriture, sync_all, rename, sync répertoire,
    /// nouveau segment, nettoyage. (shard, début µs, phases)
    pub static DIAG_SNAP: std::sync::Mutex<Vec<(u8, u64, [u64; 7])>> = std::sync::Mutex::new(Vec::new());

    pub fn diag_epoch() -> Instant {
        static T: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
        *T.get_or_init(Instant::now)
    }

    pub struct AppendTimer(Option<(Instant, Option<Instant>)>);
    impl AppendTimer {
        pub fn start() -> Self {
            Self(DIAG_ON.load(Relaxed).then(|| (Instant::now(), None)))
        }
        pub fn encoded(&mut self) {
            if let Some((_, w)) = &mut self.0 {
                *w = Some(Instant::now());
            }
        }
        pub fn finish(self, shard_id: u16) {
            if let Some((t0, Some(t1))) = self.0 {
                let c = ENCODE_WRITE_NS.get(shard_id as usize);
                c.encode.fetch_add(t1.duration_since(t0).as_nanos() as u64, Relaxed);
                c.write.fetch_add(t1.elapsed().as_nanos() as u64, Relaxed);
            }
        }
    }

    pub struct SnapTimer {
        start: Option<Instant>,
        mark: Instant,
        ph: [u64; 7],
    }
    impl SnapTimer {
        pub fn start() -> Self {
            Self { start: DIAG_ON.load(Relaxed).then(Instant::now), mark: Instant::now(), ph: [0; 7] }
        }
        pub fn lap(&mut self, i: usize) {
            self.ph[i] = self.mark.elapsed().as_micros() as u64;
            self.mark = Instant::now();
        }
        pub fn finish(self, shard_id: u16) {
            if let Some(t) = self.start {
                DIAG_SNAP.lock().unwrap().push((shard_id as u8, t.duration_since(diag_epoch()).as_micros() as u64, self.ph));
            }
        }
    }
}

#[cfg(not(feature = "wal-diagnostics"))]
mod diag {
    pub struct AppendTimer;
    impl AppendTimer {
        #[inline(always)]
        pub fn start() -> Self {
            Self
        }
        #[inline(always)]
        pub fn encoded(&mut self) {}
        #[inline(always)]
        pub fn finish(self, _shard_id: u16) {}
    }

    pub struct SnapTimer;
    impl SnapTimer {
        #[inline(always)]
        pub fn start() -> Self {
            Self
        }
        #[inline(always)]
        pub fn lap(&mut self, _i: usize) {}
        #[inline(always)]
        pub fn finish(self, _shard_id: u16) {}
    }
}

#[cfg(feature = "wal-diagnostics")]
pub use diag::{diag_encode_ns, diag_epoch, diag_write_ns, DIAG_ON, DIAG_SNAP};
use diag::{AppendTimer, SnapTimer};

impl Wal {
    pub fn last_seq(&self) -> u64 {
        self.next_seq - 1
    }

    pub fn records_since_snapshot(&self) -> u64 {
        self.since_snapshot
    }

    pub fn has_unsynced(&self) -> bool {
        self.unsynced
    }

    /// Ajoute un lot d'enregistrements en une seule écriture (group commit).
    pub fn append(&mut self, records: &[Record]) -> io::Result<()> {
        injected()?;
        let mut timer = AppendTimer::start();
        let mut buf = Vec::new();
        for rec in records {
            let payload = rec.encode_payload();
            let start = buf.len();
            buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            buf.push(RECORD_VERSION);
            buf.push(rec.record_type());
            buf.extend_from_slice(&self.shard_id.to_le_bytes());
            buf.extend_from_slice(&self.next_seq.to_le_bytes());
            buf.extend_from_slice(&rec.job_id().to_le_bytes());
            buf.extend_from_slice(&payload);
            let crc = crc32fast::hash(&buf[start..]);
            buf.extend_from_slice(&crc.to_le_bytes());
            self.next_seq += 1;
        }
        timer.encoded();
        // En cas d'échec d'I/O, le WAL est considéré comme défaillant et n'est plus utilisé.
        self.file.write_all(&buf)?;
        timer.finish(self.shard_id);
        self.since_snapshot += records.len() as u64;
        self.unsynced = true;
        Ok(())
    }

    pub fn sync(&mut self) -> io::Result<()> {
        hold_sync(self.shard_id);
        injected()?;
        if self.unsynced {
            self.file.sync_data()?;
            self.unsynced = false;
        }
        Ok(())
    }

    /// Début d'un snapshot publié par un worker : synchronise le WAL jusqu'à la frontière puis la fixe.
    /// `None` si l'état courant est déjà couvert par le dernier snapshot. Un seul snapshot à la fois par WAL.
    /// `upto` est le dernier seq écrit au moment où l'état du snapshot a été reçu par le writer : un état
    /// encodé plus tôt ne couvre jamais les records ajoutés depuis (cas d'un snapshot mis en attente).
    pub fn snapshot_begin(&mut self, upto: u64) -> io::Result<Option<SnapshotJob>> {
        self.sync()?;
        let seq = upto.min(self.last_seq());
        if seq <= self.snapshot_seq {
            return Ok(None);
        }
        Ok(Some(SnapshotJob { dir: self.dir.clone(), shard_id: self.shard_id, seq }))
    }

    /// Fin d'un snapshot dont `publish` a réussi : rotation du segment actif puis suppression des segments
    /// entièrement couverts. Les records écrits après la frontière restent dans leur segment tant que
    /// le snapshot suivant ne les couvre pas.
    pub fn snapshot_finish(&mut self, seq: u64) -> io::Result<()> {
        boundary(5)?;
        let new_first = self.next_seq;
        if new_first != self.segment_first_seq {
            // Les records écrits après la frontière sont synchronisés avant d'abandonner le segment.
            self.sync()?;
            self.file = create_segment(&self.dir, new_first)?;
            self.segment_first_seq = new_first;
        }
        boundary(6)?;
        let segments = list(&self.dir, "wal-", ".log")?;
        for pair in segments.windows(2) {
            if pair[1].0 <= seq + 1 {
                fs::remove_file(&pair[0].1)?;
            }
        }
        for (s, path) in list(&self.dir, "snap-", ".snap")? {
            if s < seq {
                fs::remove_file(path)?;
            }
        }
        self.snapshot_seq = seq;
        self.since_snapshot = self.last_seq().saturating_sub(seq);
        Ok(())
    }

    /// Écrit un snapshot de l'état courant (tous les enregistrements déjà ajoutés y sont inclus),
    /// ouvre un nouveau segment puis supprime l'historique devenu inutile.
    pub fn snapshot(&mut self, state: &[u8]) -> io::Result<()> {
        let delay = SNAPSHOT_DELAY_MS.load(std::sync::atomic::Ordering::Relaxed).max(env_snapshot_delay_ms());
        if delay > 0 {
            std::thread::sleep(std::time::Duration::from_millis(delay));
        }
        let mut timer = SnapTimer::start();
        self.sync()?;
        timer.lap(0);
        let seq = self.last_seq();
        if seq == self.snapshot_seq {
            return Ok(());
        }
        let mut head = Vec::with_capacity(32);
        head.extend_from_slice(SNAPSHOT_MAGIC);
        head.extend_from_slice(&self.shard_id.to_le_bytes());
        head.extend_from_slice(&seq.to_le_bytes());
        head.extend_from_slice(&(state.len() as u64).to_le_bytes());
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&head);
        hasher.update(state);
        let crc = hasher.finalize();

        let tmp = self.dir.join(format!("{}.tmp", snapshot_name(seq)));
        let mut f = File::create(&tmp)?;
        // Même contenu octet pour octet que l'ancien buffer unique, sans copie de `state`.
        f.write_all(&head)?;
        f.write_all(state)?;
        f.write_all(&crc.to_le_bytes())?;
        timer.lap(1);
        f.sync_all()?;
        timer.lap(2);
        fs::rename(&tmp, self.dir.join(snapshot_name(seq)))?;
        timer.lap(3);
        sync_dir(&self.dir)?;
        timer.lap(4);

        let new_first = seq + 1;
        if new_first != self.segment_first_seq {
            self.file = create_segment(&self.dir, new_first)?;
            self.segment_first_seq = new_first;
        }
        timer.lap(5);
        for (first, path) in list(&self.dir, "wal-", ".log")? {
            if first < new_first {
                fs::remove_file(path)?;
            }
        }
        for (s, path) in list(&self.dir, "snap-", ".snap")? {
            if s < seq {
                fs::remove_file(path)?;
            }
        }
        timer.lap(6);
        timer.finish(self.shard_id);
        self.snapshot_seq = seq;
        self.since_snapshot = 0;
        Ok(())
    }
}

fn create_segment(dir: &Path, first_seq: u64) -> io::Result<File> {
    let path = dir.join(segment_name(first_seq));
    let mut f = OpenOptions::new().create(true).write(true).truncate(true).open(&path)?;
    f.write_all(SEGMENT_MAGIC)?;
    f.sync_all()?;
    sync_dir(dir)?;
    drop(f);
    OpenOptions::new().append(true).open(path)
}

struct ParsedSegment {
    records: Vec<(u64, Record)>,
    valid_len: usize,
    torn: bool,
}

fn parse_segment(
    data: &[u8],
    shard_id: u16,
    is_last: bool,
    max_payload: usize,
    name: &str,
) -> Result<ParsedSegment, StorageError> {
    if data.len() < SEGMENT_MAGIC.len() {
        if is_last && SEGMENT_MAGIC.starts_with(data) {
            return Ok(ParsedSegment { records: vec![], valid_len: 0, torn: true });
        }
        return corrupt(format!("{name}: truncated segment header"));
    }
    if &data[..8] != SEGMENT_MAGIC {
        return corrupt(format!("{name}: bad segment header"));
    }
    let mut off = SEGMENT_MAGIC.len();
    let mut records = Vec::new();
    let torn = |records, off| Ok(ParsedSegment { records, valid_len: off, torn: true });
    while off < data.len() {
        let rest = &data[off..];
        if rest.len() < RECORD_HEADER_LEN {
            return if is_last { torn(records, off) } else { corrupt(format!("{name}: truncated record at {off}")) };
        }
        let len = u32::from_le_bytes(rest[0..4].try_into().unwrap()) as usize;
        if len > max_payload {
            return corrupt(format!("{name}: implausible record length {len} at offset {off}"));
        }
        let total = RECORD_HEADER_LEN + len + CRC_LEN;
        if rest.len() < total {
            return if is_last { torn(records, off) } else { corrupt(format!("{name}: truncated record at {off}")) };
        }
        let crc = u32::from_le_bytes(rest[total - CRC_LEN..total].try_into().unwrap());
        if crc32fast::hash(&rest[..total - CRC_LEN]) != crc {
            let at_eof = rest.len() == total;
            return if is_last && at_eof {
                torn(records, off)
            } else {
                corrupt(format!("{name}: checksum mismatch at offset {off}"))
            };
        }
        let (version, rtype) = (rest[4], rest[5]);
        let shard = u16::from_le_bytes(rest[6..8].try_into().unwrap());
        let seq = u64::from_le_bytes(rest[8..16].try_into().unwrap());
        let job_id = u128::from_le_bytes(rest[16..32].try_into().unwrap());
        if version != RECORD_VERSION {
            return corrupt(format!("{name}: unsupported record version {version}"));
        }
        if shard != shard_id {
            return corrupt(format!("{name}: record for shard {shard} found in shard {shard_id}"));
        }
        let rec = Record::decode(rtype, job_id, &rest[RECORD_HEADER_LEN..RECORD_HEADER_LEN + len])
            .map_err(|e| StorageError::Corrupt(format!("{name}: {e}")))?;
        records.push((seq, rec));
        off += total;
    }
    Ok(ParsedSegment { records, valid_len: off, torn: false })
}

fn read_snapshot(path: &Path, shard_id: u16) -> Result<(u64, Vec<u8>), StorageError> {
    let data = fs::read(path)?;
    let name = path.display();
    if data.len() < 8 + 2 + 8 + 8 + CRC_LEN || &data[..8] != SNAPSHOT_MAGIC {
        return corrupt(format!("{name}: bad snapshot header"));
    }
    let (body, crc) = data.split_at(data.len() - CRC_LEN);
    if crc32fast::hash(body) != u32::from_le_bytes(crc.try_into().unwrap()) {
        return corrupt(format!("{name}: snapshot checksum mismatch"));
    }
    if u16::from_le_bytes(body[8..10].try_into().unwrap()) != shard_id {
        return corrupt(format!("{name}: snapshot belongs to another shard"));
    }
    let seq = u64::from_le_bytes(body[10..18].try_into().unwrap());
    let len = u64::from_le_bytes(body[18..26].try_into().unwrap()) as usize;
    if body.len() != 26 + len {
        return corrupt(format!("{name}: snapshot length mismatch"));
    }
    Ok((seq, body[26..].to_vec()))
}

/// Récupère l'état d'un shard : dernier snapshot valide, puis replay des enregistrements WAL valides.
/// Retourne le shard (journalisation activée) et le WAL prêt à recevoir de nouvelles écritures.
pub fn recover_shard(dir: &Path, shard_id: u16, cfg: Arc<Config>) -> Result<(Shard, Wal), StorageError> {
    recover_shard_at(dir, shard_id, cfg, solid_engine_core::now_ms())
}

/// Comme `recover_shard`, avec l'instant de fin de récupération (ms UTC) fourni par l'appelant :
/// il pilote l'invalidation des réservations et la purge des jobs terminés. Les tests l'utilisent
/// avec une horloge virtuelle ; la production passe par `recover_shard` et l'horloge murale.
pub fn recover_shard_at(dir: &Path, shard_id: u16, cfg: Arc<Config>, now: u64) -> Result<(Shard, Wal), StorageError> {
    fs::create_dir_all(dir)?;
    // Les `.tmp` sont des snapshots interrompus : ils n'ont jamais été visibles.
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().is_some_and(|e| e == "tmp") {
            fs::remove_file(path)?;
        }
    }
    let max_payload = cfg.limits.max_payload_bytes + 4096;

    let mut snapshot: Option<(u64, Vec<u8>)> = None;
    let mut snapshot_errors = Vec::new();
    for (_, path) in list(dir, "snap-", ".snap")?.into_iter().rev() {
        match read_snapshot(&path, shard_id) {
            Ok(s) => {
                snapshot = Some(s);
                break;
            }
            Err(StorageError::Corrupt(m)) => snapshot_errors.push(m),
            Err(e) => return Err(e),
        }
    }
    let snapshot_seq = snapshot.as_ref().map_or(0, |s| s.0);
    let mut shard = match snapshot {
        Some((_, data)) => Shard::from_snapshot(shard_id, cfg.clone(), &data)
            .map_err(|e| StorageError::Corrupt(format!("snapshot state: {e}")))?,
        None => Shard::new(shard_id, cfg.clone()),
    };

    let segments = list(dir, "wal-", ".log")?;
    let mut expected = snapshot_seq + 1;
    let mut last_valid: Option<(PathBuf, usize, u64, bool)> = None;
    for (i, (first, path)) in segments.iter().enumerate() {
        let is_last = i + 1 == segments.len();
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        let data = fs::read(path)?;
        let parsed = parse_segment(&data, shard_id, is_last, max_payload, &name)?;
        for (seq, rec) in &parsed.records {
            if *seq < expected {
                continue; // déjà inclus dans le snapshot
            }
            if *seq != expected {
                return corrupt(format!("{name}: sequence gap (expected {expected}, found {seq})"));
            }
            shard.apply(rec);
            expected += 1;
        }
        if is_last {
            last_valid = Some((path.clone(), parsed.valid_len, *first, parsed.torn));
        }
    }
    // Un snapshot corrompu n'est toléré que si le WAL restant couvre tout l'historique nécessaire.
    match segments.first() {
        Some((first, _)) if *first > snapshot_seq + 1 => {
            return corrupt(format!("WAL starts at sequence {first} but recovery needs {}", snapshot_seq + 1))
        }
        None if !snapshot_errors.is_empty() => {
            return corrupt(format!("no valid snapshot: {}", snapshot_errors.join("; ")))
        }
        _ => {}
    }

    let (file, segment_first_seq) = match last_valid {
        Some((path, valid_len, first, torn)) => {
            if torn {
                let f = OpenOptions::new().write(true).open(&path)?;
                if valid_len < SEGMENT_MAGIC.len() {
                    f.set_len(0)?;
                    (&f).write_all(SEGMENT_MAGIC)?;
                } else {
                    f.set_len(valid_len as u64)?;
                }
                f.sync_all()?;
            }
            (OpenOptions::new().append(true).open(path)?, first)
        }
        None => (create_segment(dir, expected)?, expected),
    };
    let mut wal = Wal {
        dir: dir.to_path_buf(),
        shard_id,
        file,
        segment_first_seq,
        next_seq: expected,
        snapshot_seq,
        since_snapshot: expected - 1 - snapshot_seq,
        unsynced: false,
    };

    // Invalidation durable des réservations de l'ancienne incarnation avant toute attribution.
    shard.set_logging(true);
    shard.finish_recovery(now);
    let records = shard.take_log();
    wal.append(&records)?;
    wal.sync()?;
    Ok((shard, wal))
}

#[cfg(test)]
mod tests;
