//! camp32 : banc de propriétés étendu. Cycle de vie complet (Retry, Nack, Extend, expirations, ACK tardifs),
//! plusieurs shards, redémarrages interrompus, et matrice de coupures autour des snapshots.
//! Temps virtuel : chaque opération reçoit un `now` explicite, le modèle rejoue les mêmes échéances.
//!
//! Variables : `CAMP32_SEEDS` (défaut 200), `CAMP32_BASE` (défaut 0). Chaque échec cite sa seed.
//! Les compteurs de points d'injection atteints sont affichés en fin de test (`--nocapture`).

use solid_engine_core::config::{BackoffStrategy, Config};
use solid_engine_core::shard::Shard;
use solid_engine_core::*;
use solid_engine_storage::{recover_shard, Wal, INJECT_IO_ERROR};
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

const MAGIC_LEN: u64 = 8;
const RETRY_DELAY_MS: u64 = 500;
// Le drapeau d'injection est global au processus : les tests du fichier sont sérialisés.
static SERIAL: Mutex<()> = Mutex::new(());

static HITS: [AtomicU64; 14] = [const { AtomicU64::new(0) }; 14];
const HIT_NAMES: [&str; 14] = [
    "seeds",
    "write_error_injecte",
    "fsync_error_injecte",
    "snapshots",
    "snap_crash_avant_segment",
    "snap_crash_segment_cree",
    "snap_crash_segment_magic_partiel",
    "snap_crash_tmp_partiel",
    "snap_crash_ancien_snapshot_garde",
    "coupures_partielles",
    "recuperations_enchainees",
    "bitflip_erreur_explicite",
    "bitflip_prefixe_valide",
    "snapshot_corrompu_erreur",
];
fn hit(i: usize) {
    HITS[i].fetch_add(1, Ordering::Relaxed);
}

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
        static N: AtomicU64 = AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!("se-camp32-{tag}-{}-{}", std::process::id(), N.fetch_add(1, Ordering::Relaxed)));
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
    c.jobs.retry.strategy = BackoffStrategy::Fixed;
    c.jobs.retry.base_delay_ms = RETRY_DELAY_MS;
    c.jobs.retry.jitter = 0.0;
    // Les jobs terminés ne doivent jamais expirer pendant le test (le temps virtuel est petit).
    c.jobs.completed_retention_ms = 100_000_000_000_000;
    c.jobs.dead_retention_ms = 100_000_000_000_000;
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

fn truncate(path: &Path, len: u64) {
    OpenOptions::new().write(true).open(path).unwrap().set_len(len).unwrap();
}

/// Modèle de référence : une réimplémentation indépendante des règles de cycle de vie.
#[derive(Clone, Debug, PartialEq)]
struct J {
    state: JobState,
    attempts: u32,
    max: u32,
    res_id: u64,
    fence: u64,
    deadline: u64,
    sched: u64,
    rseq: u64,
}

#[derive(Clone, Debug, Default, PartialEq)]
struct Model {
    jobs: BTreeMap<u128, J>,
    seq: u64,
}

type View = BTreeMap<u128, (JobState, u32)>;

impl Model {
    fn tick(&mut self, now: u64) {
        let mut due: Vec<(u64, u128)> =
            self.jobs.iter().filter(|(_, j)| j.state == JobState::Scheduled && j.sched <= now).map(|(k, j)| (j.sched, *k)).collect();
        due.sort();
        for (_, id) in due {
            self.seq += 1;
            let j = self.jobs.get_mut(&id).unwrap();
            j.state = JobState::Ready;
            j.rseq = self.seq;
        }
        let mut exp: Vec<(u64, u128)> =
            self.jobs.iter().filter(|(_, j)| j.state == JobState::Claimed && j.deadline <= now).map(|(k, j)| (j.deadline, *k)).collect();
        exp.sort();
        for (_, id) in exp {
            self.retry_or_die(id, None, now);
        }
    }

    fn retry_or_die(&mut self, id: u128, delay: Option<u64>, now: u64) {
        let j = self.jobs.get_mut(&id).unwrap();
        if j.attempts >= j.max {
            j.state = JobState::Dead;
        } else {
            j.state = JobState::Scheduled;
            j.sched = now + delay.unwrap_or(RETRY_DELAY_MS);
        }
    }

    fn holds(&self, r: &Reservation) -> bool {
        self.jobs.get(&r.job_id).is_some_and(|j| j.state == JobState::Claimed && j.res_id == r.reservation_id && j.fence == r.fencing_token)
    }

    fn view(&self) -> View {
        self.jobs.iter().map(|(k, j)| (*k, (j.state, j.attempts))).collect()
    }

    /// Vue attendue après un redémarrage (horloge réelle ≫ temps virtuel) : les réservations sont
    /// abandonnées (retry immédiat, ou Dead si les tentatives sont épuisées), les échéances sont atteintes.
    fn recovered(&self) -> View {
        self.jobs
            .iter()
            .map(|(k, j)| {
                let s = match j.state {
                    JobState::Scheduled => JobState::Ready,
                    JobState::Claimed if j.attempts >= j.max => JobState::Dead,
                    JobState::Claimed => JobState::Ready,
                    s => s,
                };
                (*k, (s, j.attempts))
            })
            .collect()
    }
}

fn observed(shard: &mut Shard, upto: u128, now: u64) -> View {
    let mut out = View::new();
    for id in 1..=upto {
        if let Ok(v) = shard.get(id, now) {
            out.insert(id, (v.state, v.attempts));
        }
    }
    out
}

struct Entry {
    len: u64,
    seq: u64,
    view: View,
}

struct Sh {
    shard: Shard,
    wal: Wal,
    dir: TempDir,
    model: Model,
    entries: Vec<Entry>,
    synced: usize,
    next_id: u128,
    claims: Vec<ClaimedJob>,
    /// Tous les états vus par le modèle (après récupération), pour les cas où seul « jamais inventé » est exigé.
    history: Vec<View>,
}

impl Sh {
    fn seg_len(&self) -> u64 {
        fs::metadata(files(&self.dir.0, "wal-").pop().unwrap()).unwrap().len()
    }

    fn new(tag: &str) -> Self {
        let dir = TempDir::new(tag);
        let (shard, wal) = recover_shard(&dir.0, 0, cfg()).unwrap();
        let mut s = Self {
            shard,
            wal,
            dir,
            model: Model::default(),
            entries: vec![],
            synced: 0,
            next_id: 1,
            claims: vec![],
            history: vec![View::new()],
        };
        s.entries.push(Entry { len: s.seg_len(), seq: s.wal.last_seq(), view: View::new() });
        s
    }

    fn op(&mut self, rng: &mut Rng, now: u64, seed: u64) {
        self.model.tick(now);
        match rng.below(14) {
            0..=3 => {
                let id = self.next_id;
                self.next_id += 1;
                let max = 1 + rng.below(4) as u32;
                let req = PushRequest {
                    job_id: id,
                    channel_id: 1,
                    priority: 0,
                    max_attempts: max,
                    delay_ms: 0,
                    partition_key: vec![],
                    payload: id.to_le_bytes().to_vec(),
                };
                assert!(!self.shard.push(req, now).unwrap().duplicate, "seed {seed}");
                self.model.seq += 1;
                let rseq = self.model.seq;
                self.model.jobs.insert(id, J { state: JobState::Ready, attempts: 0, max, res_id: 0, fence: 0, deadline: 0, sched: 0, rseq });
            }
            4..=6 => {
                let lease = [100u32, 300, 1000, 2000][rng.below(4) as usize];
                let want = self.model.jobs.iter().filter(|(_, j)| j.state == JobState::Ready).min_by_key(|(_, j)| j.rseq).map(|(k, _)| *k);
                let got = self.shard.claim(1, lease, now).unwrap();
                assert_eq!(got.as_ref().map(|c| c.job_id), want, "seed {seed} t={now}: ordre FIFO du claim");
                if let Some(c) = got {
                    let j = self.model.jobs.get_mut(&c.job_id).unwrap();
                    j.state = JobState::Claimed;
                    j.attempts += 1;
                    j.fence += 1;
                    j.res_id = c.reservation_id;
                    j.deadline = now + lease as u64;
                    assert_eq!(c.lease_deadline, j.deadline, "seed {seed}: échéance du bail");
                    assert_eq!((c.fencing_token, c.attempts), (j.fence, j.attempts), "seed {seed}: fencing/tentatives");
                    self.claims.push(c);
                }
            }
            7..=11 => {
                // Réservation quelconque parmi toutes celles délivrées : valide, expirée ou périmée (ACK tardif).
                if self.claims.is_empty() {
                    return;
                }
                let c = &self.claims[rng.below(self.claims.len() as u64) as usize];
                let r = Reservation { job_id: c.job_id, reservation_id: c.reservation_id, fencing_token: c.fencing_token };
                let valid = self.model.holds(&r);
                match rng.below(5) {
                    0 | 1 => {
                        let res = self.shard.ack(r, now);
                        assert_eq!(res.is_ok(), valid, "seed {seed} t={now}: ack({}) valide={valid}", r.job_id);
                        if valid {
                            self.model.jobs.get_mut(&r.job_id).unwrap().state = JobState::Completed;
                        }
                    }
                    2 => {
                        let kind = match rng.below(3) {
                            0 => NackKind::Fail,
                            1 => NackKind::Retry { delay_ms: None },
                            _ => NackKind::Retry { delay_ms: Some(rng.below(900)) },
                        };
                        let res = self.shard.nack(r, kind, now);
                        assert_eq!(res.is_ok(), valid, "seed {seed} t={now}: nack valide={valid}");
                        if valid {
                            match kind {
                                NackKind::Fail => self.model.jobs.get_mut(&r.job_id).unwrap().state = JobState::Dead,
                                NackKind::Retry { delay_ms } => self.model.retry_or_die(r.job_id, delay_ms, now),
                            }
                        }
                    }
                    _ => {
                        let lease = [100u32, 1000, 2000][rng.below(3) as usize];
                        let res = self.shard.extend_lease(r, lease, now);
                        assert_eq!(res.is_ok(), valid, "seed {seed} t={now}: extend valide={valid}");
                        if valid {
                            let j = self.model.jobs.get_mut(&r.job_id).unwrap();
                            j.deadline = j.deadline.max(now + lease as u64);
                            assert_eq!(res.unwrap(), j.deadline, "seed {seed}: bail jamais raccourci");
                        }
                    }
                }
            }
            _ => {
                if self.next_id == 1 {
                    return;
                }
                let id = 1 + rng.below(self.next_id as u64 - 1) as u128;
                let valid = self.model.jobs.get(&id).is_some_and(|j| matches!(j.state, JobState::Ready | JobState::Scheduled));
                let res = self.shard.cancel(id, now);
                assert_eq!(res.is_ok(), valid, "seed {seed} t={now}: cancel({id}) valide={valid}");
                if valid {
                    self.model.jobs.get_mut(&id).unwrap().state = JobState::Cancelled;
                }
            }
        }
    }

    /// Compare l'état vivant du moteur au modèle (les deux avancent à `now`).
    fn check_live(&mut self, now: u64, seed: u64) {
        self.model.tick(now);
        let got = observed(&mut self.shard, self.next_id.saturating_sub(1), now);
        assert_eq!(got, self.model.view(), "seed {seed} t={now}: état vivant ≠ modèle");
    }

    fn persist(&mut self, sync: bool, seed: u64) -> std::io::Result<()> {
        let log = self.shard.take_log();
        let before = self.wal.last_seq();
        self.wal.append(&log)?;
        assert_eq!(self.wal.last_seq(), before + log.len() as u64, "seed {seed}: séquence WAL non contiguë");
        self.entries.push(Entry { len: self.seg_len(), seq: self.wal.last_seq(), view: self.model.recovered() });
        self.history.push(self.model.recovered());
        if sync {
            self.wal.sync()?;
            self.synced = self.entries.len() - 1;
        }
        Ok(())
    }
}

fn check_recover(d: &Path, want: &View, want_seq: Option<u64>, upto: u128, what: &str, seed: u64) {
    let (mut shard, wal) = recover_shard(d, 0, cfg()).unwrap_or_else(|e| panic!("seed {seed} {what}: {e:?}"));
    assert_eq!(&observed(&mut shard, upto, 4_000_000_000_000), want, "seed {seed} {what}");
    if let Some(s) = want_seq {
        // finish_recovery journalise des Retry/Dead/promotions : la séquence ne peut qu'avancer.
        assert!(wal.last_seq() >= s, "seed {seed} {what}: séquence récupérée {} < préfixe {s}", wal.last_seq());
    }
}

fn prefix_entry(sh: &Sh, len: u64) -> &Entry {
    sh.entries.iter().rev().find(|e| e.len <= len).unwrap_or(&sh.entries[0])
}

fn crash_checks(sh: &Sh, rng: &mut Rng, seed: u64, tag: &str) {
    let upto = sh.next_id.saturating_sub(1);
    let len = sh.seg_len();
    let copy = |t: &str| {
        let d = TempDir::new(t);
        copy_dir(&sh.dir.0, &d.0);
        d
    };
    // SIGKILL : tout ce qui est écrit survit. Perte du non-synchronisé : l'état synchronisé est exact.
    for (l, what) in [(len, "sigkill"), (sh.entries[sh.synced].len, "synced")] {
        let d = copy(what);
        truncate(&files(&d.0, "wal-").pop().unwrap(), l);
        let e = prefix_entry(sh, l);
        check_recover(&d.0, &e.view, Some(e.seq), upto, &format!("{tag} {what}"), seed);
    }
    // Écritures partielles à des positions arbitraires.
    for _ in 0..4 {
        let l = MAGIC_LEN + rng.below(len - MAGIC_LEN + 1);
        let d = copy("partial");
        truncate(&files(&d.0, "wal-").pop().unwrap(), l);
        let e = prefix_entry(sh, l);
        check_recover(&d.0, &e.view, Some(e.seq), upto, &format!("{tag} partial len={l}"), seed);
        hit(9);
    }
    // Redémarrages interrompus : recoveries successives, chacune suivie d'une nouvelle coupure plus courte.
    {
        let d = copy("chain");
        let seg = files(&d.0, "wal-").pop().unwrap();
        let mut cur = len;
        // Après une récupération, le fichier contient en plus des records de récupération qui ne changent pas la vue :
        // on suit donc le préfixe effectif plutôt que la longueur brute.
        let mut eff = prefix_entry(sh, len);
        for round in 0..3 {
            let l = MAGIC_LEN + rng.below(cur - MAGIC_LEN + 1);
            truncate(&seg, l);
            if l < eff.len {
                eff = prefix_entry(sh, l);
            }
            check_recover(&d.0, &eff.view, None, upto, &format!("{tag} chain#{round} len={l}"), seed);
            cur = fs::metadata(&seg).unwrap().len();
            hit(10);
        }
        // Le journal récupéré reste appendable : un nouveau Push survit à un nouveau crash.
        let (mut shard, mut wal) = recover_shard(&d.0, 0, cfg()).unwrap();
        let id = sh.next_id;
        shard
            .push(
                PushRequest { job_id: id, channel_id: 1, priority: 0, max_attempts: 2, delay_ms: 0, partition_key: vec![], payload: vec![] },
                now_ms(),
            )
            .unwrap();
        let mut want = eff.view.clone();
        want.insert(id, (JobState::Ready, 0));
        wal.append(&shard.take_log()).unwrap();
        wal.sync().unwrap();
        drop((shard, wal));
        check_recover(&d.0, &want, None, id, &format!("{tag} appendable"), seed);
    }
    // Bit flip : erreur explicite, ou état déjà vu par le modèle ; jamais un état inventé.
    for _ in 0..3 {
        let d = copy("flip");
        let seg = files(&d.0, "wal-").pop().unwrap();
        let mut bytes = fs::read(&seg).unwrap();
        if bytes.len() as u64 <= MAGIC_LEN {
            continue;
        }
        let pos = MAGIC_LEN as usize + rng.below(bytes.len() as u64 - MAGIC_LEN) as usize;
        bytes[pos] ^= 1 << rng.below(8);
        fs::write(&seg, bytes).unwrap();
        match recover_shard(&d.0, 0, cfg()) {
            Err(_) => hit(11),
            Ok((mut shard, _)) => {
                let got = observed(&mut shard, upto, 4_000_000_000_000);
                assert!(sh.history.contains(&got), "seed {seed} {tag}: bit flip à {pos} → état jamais produit par le modèle");
                hit(12);
            }
        }
    }
}

/// Matrice de coupures autour d'un snapshot : chaque état intermédiaire possible doit se récupérer exactement.
fn snapshot_crash_matrix(before: &Path, sh: &Sh, expect: &View, seed: u64) {
    let after = &sh.dir.0;
    let upto = sh.next_id.saturating_sub(1);
    // Snapshot sans effet (aucun record depuis le précédent) : rien à couper.
    let Some(snap) = files(after, "snap-").pop().filter(|p| files(before, "snap-").pop().as_ref().map(|b| b.file_name()) != Some(p.file_name())) else {
        return;
    };
    let new_seg = files(after, "wal-").pop().unwrap();
    let want_seq = sh.wal.last_seq();
    let add_snap = |d: &Path| {
        fs::copy(&snap, d.join(snap.file_name().unwrap())).unwrap();
    };
    let mut variants: Vec<(&str, usize, TempDir)> = vec![];
    // Avant la publication : seul un .tmp partiel existe.
    let d = TempDir::new("snap-tmp");
    copy_dir(before, &d.0);
    let data = fs::read(&snap).unwrap();
    fs::write(d.0.join(format!("{}.tmp", snap.file_name().unwrap().to_string_lossy())), &data[..data.len() / 2]).unwrap();
    variants.push(("tmp_partiel", 7, d));
    // Snapshot publié, ancien segment encore là, nouveau segment absent.
    let d = TempDir::new("snap-pub");
    copy_dir(before, &d.0);
    add_snap(&d.0);
    variants.push(("publie_sans_nouveau_segment", 4, d));
    // Nouveau segment créé, ancien pas encore supprimé.
    let d = TempDir::new("snap-seg");
    copy_dir(before, &d.0);
    add_snap(&d.0);
    fs::copy(&new_seg, d.0.join(new_seg.file_name().unwrap())).unwrap();
    variants.push(("nouveau_segment_cree", 5, d));
    // Nouveau segment à peine créé : en-tête magique partiel.
    for k in [0u64, 3, 7] {
        let d = TempDir::new("snap-magic");
        copy_dir(before, &d.0);
        add_snap(&d.0);
        let p = d.0.join(new_seg.file_name().unwrap());
        fs::copy(&new_seg, &p).unwrap();
        truncate(&p, k);
        variants.push(("segment_magic_partiel", 6, d));
    }
    // Ancien snapshot non encore supprimé (si présent avant).
    if let Some(old) = files(before, "snap-").pop() {
        let d = TempDir::new("snap-old");
        copy_dir(&sh.dir.0, &d.0);
        fs::copy(&old, d.0.join(old.file_name().unwrap())).unwrap();
        variants.push(("ancien_snapshot_garde", 8, d));
    }
    for (what, h, d) in variants {
        // Le snapshot recouvre exactement l'état courant : peu importe la variante, l'état récupéré est celui-là.
        check_recover(&d.0, expect, Some(want_seq), upto, &format!("snapshot {what}"), seed);
        hit(h);
    }
    // Snapshot corrompu une fois le WAL antérieur supprimé : erreur explicite, jamais d'état inventé.
    let d = TempDir::new("snap-bad");
    copy_dir(after, &d.0);
    let p = files(&d.0, "snap-").pop().unwrap();
    let mut bytes = fs::read(&p).unwrap();
    let pos = (seed as usize * 31 + 17) % bytes.len();
    bytes[pos] ^= 0x40;
    fs::write(&p, bytes).unwrap();
    match recover_shard(&d.0, 0, cfg()) {
        Err(_) => hit(13),
        Ok((mut shard, _)) => {
            let got = observed(&mut shard, upto, 4_000_000_000_000);
            assert!(sh.history.contains(&got), "seed {seed}: snapshot corrompu → état jamais produit");
        }
    }
}

fn one_seed(seed: u64) {
    hit(0);
    let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
    let n_shards = 1 + rng.below(3) as usize;
    let mut shards: Vec<Sh> = (0..n_shards).map(|i| Sh::new(&format!("s{i}"))).collect();
    let n_ops = 30 + rng.below(70);
    let fault_at = if rng.below(3) == 0 { Some((rng.below(n_ops), rng.below(n_shards as u64) as usize, rng.below(2))) } else { None };
    let mut now = 1_000u64;
    let mut snap_checks: Vec<(TempDir, usize, View)> = vec![];
    'steps: for step in 0..n_ops {
        now += rng.below(900);
        let target = rng.below(n_shards as u64) as usize;
        shards[target].op(&mut rng, now, seed);
        for (i, sh) in shards.iter_mut().enumerate() {
            sh.check_live(now, seed);
            let injected = fault_at.filter(|(s, t, _)| *s == step && *t == i);
            match injected {
                Some((_, _, 0)) => {
                    // Erreur d'écriture : propagée, rien n'est écrit ; le moteur serait en échec.
                    INJECT_IO_ERROR.store(true, Ordering::SeqCst);
                    let r = sh.persist(false, seed);
                    INJECT_IO_ERROR.store(false, Ordering::SeqCst);
                    assert!(r.is_err(), "seed {seed}: erreur d'écriture injectée non propagée");
                    hit(1);
                    break 'steps;
                }
                Some((_, _, _)) => {
                    sh.persist(false, seed).unwrap();
                    INJECT_IO_ERROR.store(true, Ordering::SeqCst);
                    let r = sh.wal.sync();
                    INJECT_IO_ERROR.store(false, Ordering::SeqCst);
                    assert!(r.is_err(), "seed {seed}: erreur de fsync injectée non propagée");
                    hit(2);
                    break 'steps;
                }
                None => {}
            }
            let sync = rng.below(100) < 30;
            sh.persist(sync, seed).unwrap();
            if rng.below(25) == 0 {
                sh.persist(true, seed).unwrap();
                let before = TempDir::new("snap-before");
                copy_dir(&sh.dir.0, &before.0);
                sh.wal.snapshot(&sh.shard.encode_snapshot()).unwrap();
                hit(3);
                let expect = sh.model.recovered();
                sh.entries = vec![Entry { len: sh.seg_len(), seq: sh.wal.last_seq(), view: expect.clone() }];
                sh.synced = 0;
                snapshot_crash_matrix(&before.0, sh, &expect, seed);
                snap_checks.push((before, i, expect));
            }
        }
    }
    for (i, sh) in shards.iter().enumerate() {
        crash_checks(sh, &mut rng, seed, &format!("shard{i}/{n_shards}"));
    }
    // Isolation : chaque shard récupère son propre état, indépendamment des autres, dans le même répertoire de test.
    for sh in shards.iter() {
        let e = &sh.entries[sh.entries.len() - 1];
        check_recover(&sh.dir.0, &e.view, Some(e.seq), sh.next_id.saturating_sub(1), "isolation shard", seed);
    }
}

fn seeds() -> std::ops::Range<u64> {
    let n: u64 = std::env::var("CAMP32_SEEDS").ok().and_then(|v| v.parse().ok()).unwrap_or(200);
    let base: u64 = std::env::var("CAMP32_BASE").ok().and_then(|v| v.parse().ok()).unwrap_or(0);
    base..base + n
}

#[test]
fn lifecycle_multishard_snapshot_matrix_matches_reference_model() {
    let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let r = seeds();
    println!("camp32 seeds {}..{}", r.start, r.end);
    for seed in r {
        one_seed(seed);
    }
    for (n, h) in HIT_NAMES.iter().zip(HITS.iter()) {
        println!("camp32 point_atteint {n} = {}", h.load(Ordering::Relaxed));
    }
}
