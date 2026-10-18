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
        // En cas d'échec d'I/O, le WAL est considéré comme défaillant et n'est plus utilisé.
        self.file.write_all(&buf)?;
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

    /// Écrit un snapshot de l'état courant (tous les enregistrements déjà ajoutés y sont inclus),
    /// ouvre un nouveau segment puis supprime l'historique devenu inutile.
    pub fn snapshot(&mut self, state: &[u8]) -> io::Result<()> {
        self.sync()?;
        let seq = self.last_seq();
        if seq == self.snapshot_seq {
            return Ok(());
        }
        let mut data = Vec::with_capacity(state.len() + 40);
        data.extend_from_slice(SNAPSHOT_MAGIC);
        data.extend_from_slice(&self.shard_id.to_le_bytes());
        data.extend_from_slice(&seq.to_le_bytes());
        data.extend_from_slice(&(state.len() as u64).to_le_bytes());
        data.extend_from_slice(state);
        let crc = crc32fast::hash(&data);
        data.extend_from_slice(&crc.to_le_bytes());

        let tmp = self.dir.join(format!("{}.tmp", snapshot_name(seq)));
        let mut f = File::create(&tmp)?;
        f.write_all(&data)?;
        f.sync_all()?;
        fs::rename(&tmp, self.dir.join(snapshot_name(seq)))?;
        sync_dir(&self.dir)?;

        let new_first = seq + 1;
        if new_first != self.segment_first_seq {
            self.file = create_segment(&self.dir, new_first)?;
            self.segment_first_seq = new_first;
        }
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
    shard.finish_recovery(solid_engine_core::now_ms());
    let records = shard.take_log();
    wal.append(&records)?;
    wal.sync()?;
    Ok((shard, wal))
}

#[cfg(test)]
mod tests;
