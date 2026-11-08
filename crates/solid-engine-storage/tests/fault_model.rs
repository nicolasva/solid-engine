//! camp31 : tests de propriétés du WAL contre un modèle de référence indépendant, avec coupures simulées
//! (préfixes, écritures partielles, bit flips) et erreurs d'I/O injectées. Séquences reproductibles par seed.
//!
//! `CAMP31_SEEDS` (défaut 200) et `CAMP31_BASE` (défaut 0) pilotent la campagne ; chaque échec cite sa seed.

use solid_engine_core::config::Config;
use solid_engine_core::shard::Shard;
use solid_engine_core::*;
use solid_engine_storage::{recover_shard, Wal, INJECT_IO_ERROR};
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

const MAGIC_LEN: u64 = 8;
// Le drapeau d'injection est global au processus : les tests du fichier sont sérialisés.
static SERIAL: Mutex<()> = Mutex::new(());

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

struct TempDir(PathBuf);
impl TempDir {
    fn new(tag: &str) -> Self {
        use std::sync::atomic::AtomicU64;
        static N: AtomicU64 = AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "se-camp31-{tag}-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
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

fn files(dir: &Path, prefix: &str) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.file_name().unwrap().to_string_lossy().starts_with(prefix))
        .collect();
    v.sort();
    v
}

fn copy_dir(from: &Path, to: &Path) {
    for e in fs::read_dir(from).unwrap() {
        let e = e.unwrap();
        fs::copy(e.path(), to.join(e.file_name())).unwrap();
    }
}

/// Modèle de référence : états attendus par job, sans réutiliser le moteur.
type Model = BTreeMap<u128, JobState>;

/// Après récupération, les réservations sont invalidées : un job réservé redevient prêt.
fn after_recovery(m: &Model) -> Model {
    m.iter().map(|(k, v)| (*k, if *v == JobState::Claimed { JobState::Ready } else { *v })).collect()
}

fn observed(shard: &mut Shard, upto: u128) -> Model {
    let mut out = Model::new();
    for id in 1..=upto {
        if let Ok(v) = shard.get(id, now_ms()) {
            out.insert(id, v.state);
        }
    }
    out
}

fn push_req(id: u128) -> PushRequest {
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

/// Point de cohérence du journal : taille du segment courant une fois l'opération écrite, et état attendu.
struct Entry {
    len: u64,
    model: Model,
}

struct Run {
    shard: Shard,
    wal: Wal,
    dir: TempDir,
    model: Model,
    entries: Vec<Entry>,
    synced: usize,
    next_id: u128,
    claims: Vec<ClaimedJob>,
}

impl Run {
    fn seg_len(&self) -> u64 {
        fs::metadata(files(&self.dir.0, "wal-").pop().unwrap()).unwrap().len()
    }

    fn new(tag: &str) -> Self {
        let dir = TempDir::new(tag);
        let (shard, wal) = recover_shard(&dir.0, 0, cfg()).unwrap();
        let mut r = Self { shard, wal, dir, model: Model::new(), entries: vec![], synced: 0, next_id: 1, claims: vec![] };
        r.entries.push(Entry { len: r.seg_len(), model: Model::new() });
        r
    }

    /// Applique une opération aléatoire au moteur et au modèle, puis vérifie leur accord immédiat.
    fn op(&mut self, rng: &mut Rng, seed: u64) {
        let now = now_ms();
        match rng.below(10) {
            0..=3 => {
                let id = self.next_id;
                self.next_id += 1;
                assert!(!self.shard.push(push_req(id), now).unwrap().duplicate, "seed {seed}");
                self.model.insert(id, JobState::Ready);
            }
            4..=6 => {
                let oldest_ready = self.model.iter().find(|(_, s)| **s == JobState::Ready).map(|(k, _)| *k);
                let got = self.shard.claim(1, 60_000, now).unwrap();
                assert_eq!(got.as_ref().map(|c| c.job_id), oldest_ready, "seed {seed}: claim FIFO");
                if let Some(c) = got {
                    self.model.insert(c.job_id, JobState::Claimed);
                    self.claims.push(c);
                }
            }
            7 | 8 => {
                if let Some(c) = self.claims.pop() {
                    let r = Reservation { job_id: c.job_id, reservation_id: c.reservation_id, fencing_token: c.fencing_token };
                    self.shard.ack(r, now).unwrap();
                    self.model.insert(c.job_id, JobState::Completed);
                }
            }
            _ => {
                let id = 1 + rng.below(self.next_id as u64) as u128;
                let expect_ok = self.model.get(&id) == Some(&JobState::Ready);
                let r = self.shard.cancel(id, now);
                assert_eq!(r.is_ok(), expect_ok, "seed {seed}: cancel({id}) vs modèle {:?}", self.model.get(&id));
                if r.is_ok() {
                    self.model.insert(id, JobState::Cancelled);
                }
            }
        }
    }

    /// Écrit le journal de l'opération (appliquée, mise en file, écrite) puis, parfois, le synchronise.
    fn persist(&mut self, sync: bool) -> std::io::Result<()> {
        let log = self.shard.take_log();
        self.wal.append(&log)?;
        self.entries.push(Entry { len: self.seg_len(), model: self.model.clone() });
        if sync {
            self.wal.sync()?;
            self.synced = self.entries.len() - 1;
        }
        Ok(())
    }
}

fn truncated_copy(run: &Run, len: u64, tag: &str) -> TempDir {
    let d = TempDir::new(tag);
    copy_dir(&run.dir.0, &d.0);
    let seg = files(&d.0, "wal-").pop().unwrap();
    OpenOptions::new().write(true).open(seg).unwrap().set_len(len).unwrap();
    d
}

/// Récupère la copie ; le résultat doit être exactement l'état du dernier point de cohérence contenu dans le préfixe.
fn check_prefix(run: &Run, len: u64, tag: &str, seed: u64) -> TempDir {
    let d = truncated_copy(run, len, tag);
    let want = run.entries.iter().rev().find(|e| e.len <= len).map(|e| e.model.clone()).unwrap_or_default();
    let (mut shard, _) = recover_shard(&d.0, 0, cfg()).unwrap_or_else(|e| panic!("seed {seed} {tag} len={len}: {e:?}"));
    assert_eq!(observed(&mut shard, run.next_id), after_recovery(&want), "seed {seed} {tag} len={len}");
    d
}

fn one_seed(seed: u64) {
    let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
    let mut run = Run::new("run");
    let n_ops = 20 + rng.below(60);
    for _ in 0..n_ops {
        match rng.below(40) {
            0 => {
                // Instantané : le nouveau segment repart de l'état courant, déjà écrit et synchronisé.
                run.persist(true).unwrap();
                run.wal.snapshot(&run.shard.encode_snapshot()).unwrap();
                run.entries = vec![Entry { len: run.seg_len(), model: run.model.clone() }];
                run.synced = 0;
            }
            1 => {
                // Erreur d'écriture injectée : l'appel échoue, le moteur serait en échec, rien n'est écrit.
                run.op(&mut rng, seed);
                INJECT_IO_ERROR.store(true, Ordering::SeqCst);
                let r = run.persist(false);
                INJECT_IO_ERROR.store(false, Ordering::SeqCst);
                assert!(r.is_err(), "seed {seed}: l'erreur d'écriture injectée doit être propagée");
                break;
            }
            2 => {
                // Erreur de synchronisation : l'écriture a eu lieu, mais rien ne doit être déclaré durable.
                run.op(&mut rng, seed);
                run.persist(false).unwrap();
                INJECT_IO_ERROR.store(true, Ordering::SeqCst);
                let r = run.wal.sync();
                INJECT_IO_ERROR.store(false, Ordering::SeqCst);
                assert!(r.is_err(), "seed {seed}: l'erreur de fsync injectée doit être propagée");
                break;
            }
            _ => {
                run.op(&mut rng, seed);
                let sync = rng.below(100) < 30;
                run.persist(sync).unwrap();
            }
        }
    }

    let len = run.seg_len();
    let synced_len = run.entries[run.synced].len;

    // SIGKILL : tout le contenu écrit survit au crash du processus.
    let d = check_prefix(&run, len, "sigkill", seed);
    // Perte de tout ce qui n'est pas synchronisé (coupure d'alimentation idéale) : l'état synchronisé est exact.
    check_prefix(&run, synced_len, "synced", seed);
    // Écritures partielles : coupure à des positions arbitraires du segment courant.
    for _ in 0..6 {
        let l = MAGIC_LEN + rng.below(len - MAGIC_LEN + 1);
        check_prefix(&run, l, "partial", seed);
    }
    // Coupure juste avant la fin et au milieu du dernier enregistrement.
    for l in [len.saturating_sub(1), len.saturating_sub(5)] {
        if l >= MAGIC_LEN {
            check_prefix(&run, l, "tail", seed);
        }
    }

    // Le journal récupéré reste appendable et stable à travers des redémarrages répétés.
    let (mut shard, mut wal) = recover_shard(&d.0, 0, cfg()).unwrap();
    let mut want = after_recovery(&run.entries.last().unwrap().model);
    let id = run.next_id;
    shard.push(push_req(id), now_ms()).unwrap();
    want.insert(id, JobState::Ready);
    wal.append(&shard.take_log()).unwrap();
    wal.sync().unwrap();
    drop((shard, wal));
    for _ in 0..3 {
        let (mut shard, wal) = recover_shard(&d.0, 0, cfg()).unwrap();
        assert_eq!(observed(&mut shard, id), want, "seed {seed}: redémarrage répété");
        drop(wal);
    }

    // Bit flip : la récupération échoue explicitement ou retombe sur un préfixe valide, jamais sur un état inventé.
    for _ in 0..4 {
        let d = TempDir::new("flip");
        copy_dir(&run.dir.0, &d.0);
        let seg = files(&d.0, "wal-").pop().unwrap();
        let mut bytes = fs::read(&seg).unwrap();
        if bytes.len() as u64 <= MAGIC_LEN {
            continue;
        }
        let pos = MAGIC_LEN as usize + rng.below(bytes.len() as u64 - MAGIC_LEN) as usize;
        bytes[pos] ^= 1 << rng.below(8);
        fs::write(&seg, bytes).unwrap();
        if let Ok((mut shard, _)) = recover_shard(&d.0, 0, cfg()) {
            let got = observed(&mut shard, run.next_id);
            let ok = run.entries.iter().any(|e| after_recovery(&e.model) == got);
            assert!(ok, "seed {seed}: bit flip à {pos} a produit un état hors de tout préfixe valide");
        }
    }
}

fn seeds() -> std::ops::Range<u64> {
    let n: u64 = std::env::var("CAMP31_SEEDS").ok().and_then(|v| v.parse().ok()).unwrap_or(200);
    let base: u64 = std::env::var("CAMP31_BASE").ok().and_then(|v| v.parse().ok()).unwrap_or(0);
    base..base + n
}

#[test]
fn wal_matches_reference_model_under_crashes_and_faults() {
    let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    for seed in seeds() {
        one_seed(seed);
    }
}
