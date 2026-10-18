//! Runtime du moteur : un thread par shard (shared-nothing), mailboxes bornées,
//! routage des commandes et réveil événementiel des workers en attente.

use solid_engine_core::config::{Config, Durability};
use solid_engine_core::shard::Shard;
use solid_engine_storage::{lock_data_dir, recover_shard, DataDirLock, Wal};
use std::path::Path;
use solid_engine_core::*;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// Canal de réponse d'une commande : un callback appelé une seule fois par le thread qui
/// libère la réponse (shard ou writer WAL). Il doit rester court et ne jamais bloquer.
pub struct Reply<T>(Box<dyn FnOnce(Result<T>) + Send>);

impl<T> Reply<T> {
    fn send(self, result: Result<T>) -> std::result::Result<(), ()> {
        (self.0)(result);
        Ok(())
    }
}

impl<T: Send + 'static> Reply<T> {
    fn new(f: impl FnOnce(Result<T>) + Send + 'static) -> Self {
        Self(Box::new(f))
    }
}

enum Cmd {
    Push(PushRequest, Reply<PushOutcome>),
    Claim { channel_id: u16, lease_ms: u32, reply: Reply<Option<ClaimedJob>> },
    Ack(Reservation, Reply<()>),
    Nack(Reservation, NackKind, Reply<()>),
    Extend(Reservation, u32, Reply<u64>),
    Get(u128, Reply<JobView>),
    Cancel(u128, Reply<()>),
    Stats(Option<u16>, Reply<Stats>),
    Shutdown,
}

/// Compteur de génération + condvar : les workers en attente sont réveillés quand un job
/// devient disponible. La notification n'est jamais une source de vérité (le worker re-claim).
#[derive(Default)]
struct Notifier {
    generation: Mutex<u64>,
    cv: Condvar,
}

impl Notifier {
    fn notify(&self) {
        *self.generation.lock().unwrap() += 1;
        self.cv.notify_all();
    }

    fn generation(&self) -> u64 {
        *self.generation.lock().unwrap()
    }

    fn wait_change(&self, seen: u64, timeout: Duration) {
        let g = self.generation.lock().unwrap();
        let _ = self.cv.wait_timeout_while(g, timeout, |g| *g == seen).unwrap();
    }
}

pub struct Engine {
    cfg: Arc<Config>,
    shards: Vec<SyncSender<Cmd>>,
    threads: Mutex<Vec<JoinHandle<()>>>,
    notifier: Arc<Notifier>,
    next_claim_shard: AtomicUsize,
    id_counter: AtomicU64,
    shutting_down: AtomicBool,
    lock: Mutex<Option<DataDirLock>>,
    /// Prototype Zero-Dispatch : shard 0 (et son WAL) confié au réacteur.
    direct: Mutex<Option<(Shard, Option<Durable>)>>,
}

pub(crate) fn direct_enabled() -> bool {
    std::env::var("SOLID_ENGINE_REACTOR_DIRECT").is_ok()
}

impl Engine {
    pub fn start(cfg: Config) -> std::result::Result<Arc<Self>, String> {
        cfg.validate()?;
        let cfg = Arc::new(cfg);
        let lock = match (&cfg.storage.durability, &cfg.storage.data_dir) {
            (Durability::Memory, _) | (_, None) => None,
            (_, Some(dir)) => {
                Some(lock_data_dir(Path::new(dir), cfg.engine.shards).map_err(|e| e.to_string())?)
            }
        };
        let notifier = Arc::new(Notifier::default());
        let (mut senders, mut threads) = (Vec::new(), Vec::new());
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let mut direct = None;
        let spawned = if direct_enabled() {
            if cfg.engine.shards != 1 {
                return Err("SOLID_ENGINE_REACTOR_DIRECT exige un seul shard".into());
            }
            direct = Some(open_shard(0, &cfg)?);
            senders.push(sync_channel(1).0);
            0
        } else {
            cfg.engine.shards
        };
        for id in 0..spawned {
            let (tx, rx) = sync_channel(cfg.engine.mailbox_capacity);
            let (n, c, r) = (notifier.clone(), cfg.clone(), ready_tx.clone());
            let t = std::thread::Builder::new()
                .name(format!("shard-{id}"))
                .spawn(move || shard_thread(id, c, rx, n, r))
                .map_err(|e| e.to_string())?;
            senders.push(tx);
            threads.push(t);
        }
        drop(ready_tx);
        // La récupération des shards est parallèle ; aucune connexion n'est acceptée avant la fin.
        for _ in 0..spawned {
            match ready_rx.recv() {
                Ok(Ok(())) => {}
                Ok(Err(e)) => return Err(e),
                Err(_) => return Err("a shard thread terminated during recovery".into()),
            }
        }
        Ok(Arc::new(Self {
            cfg,
            shards: senders,
            threads: Mutex::new(threads),
            notifier,
            next_claim_shard: AtomicUsize::new(0),
            id_counter: AtomicU64::new(1),
            shutting_down: AtomicBool::new(false),
            lock: Mutex::new(lock),
            direct: Mutex::new(direct),
        }))
    }

    pub(crate) fn take_direct(&self) -> Option<(Shard, Option<Durable>)> {
        self.direct.lock().unwrap().take()
    }

    pub fn config(&self) -> &Config {
        &self.cfg
    }

    pub fn shutdown(&self) {
        self.shutting_down.store(true, Ordering::SeqCst);
        for s in &self.shards {
            let _ = s.send(Cmd::Shutdown);
        }
        self.notifier.notify();
        for t in self.threads.lock().unwrap().drain(..) {
            let _ = t.join();
        }
        self.lock.lock().unwrap().take();
    }

    pub fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::SeqCst)
    }

    fn call<T: Send + 'static>(&self, shard: usize, make: impl FnOnce(Reply<T>) -> Cmd) -> Result<T> {
        let (tx, rx) = sync_channel(1);
        let reply = Reply::new(move |r| {
            let _ = tx.send(r);
        });
        match self.shards[shard].try_send(make(reply)) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                return Err(EngineError::new(ErrorCode::Overloaded, "shard mailbox is full"))
            }
            Err(TrySendError::Disconnected(_)) => return Err(shutting_down()),
        }
        rx.recv().map_err(|_| shutting_down())?
    }

    /// Variante sans attente : `done` est appelé quand la réponse est libérée (après le fsync
    /// en `strict`). Il s'exécute sur un thread du moteur et ne doit donc jamais bloquer.
    fn call_async<T: Send + 'static>(
        &self,
        shard: usize,
        make: impl FnOnce(Reply<T>) -> Cmd,
        done: impl FnOnce(Result<T>) + Send + 'static,
    ) {
        type Slot<T> = Arc<Mutex<Option<Box<dyn FnOnce(Result<T>) + Send>>>>;
        let slot: Slot<T> = Arc::new(Mutex::new(Some(Box::new(done))));
        let from_shard = slot.clone();
        let reply = Reply::new(move |r| {
            if let Some(done) = from_shard.lock().unwrap().take() {
                done(r);
            }
        });
        let error = match self.shards[shard].try_send(make(reply)) {
            Ok(()) => return,
            Err(TrySendError::Full(_)) => EngineError::new(ErrorCode::Overloaded, "shard mailbox is full"),
            Err(TrySendError::Disconnected(_)) => shutting_down(),
        };
        let done = slot.lock().unwrap().take();
        if let Some(done) = done {
            done(Err(error));
        }
    }

    fn push_shard(&self, req: &mut PushRequest) -> usize {
        if req.job_id == 0 {
            req.job_id = self.generate_id();
        }
        if req.partition_key.is_empty() {
            shard_for(req.channel_id, &req.job_id.to_le_bytes(), self.shards.len())
        } else {
            shard_for(req.channel_id, &req.partition_key, self.shards.len())
        }
    }

    pub fn push(&self, mut req: PushRequest) -> Result<PushOutcome> {
        let shard = self.push_shard(&mut req);
        self.call(shard, |r| Cmd::Push(req, r))
    }

    /// Renvoie le shard visé, pour que l'appelant puisse y rattacher ses réservations suivantes.
    pub fn push_async(&self, mut req: PushRequest, done: impl FnOnce(Result<PushOutcome>) + Send + 'static) -> usize {
        let shard = self.push_shard(&mut req);
        self.call_async(shard, |r| Cmd::Push(req, r), done);
        shard
    }

    /// Un essai de réservation sur tous les shards, en commençant par un shard tournant.
    pub fn try_claim(&self, channel_id: u16, lease_ms: u32) -> Result<Option<ClaimedJob>> {
        let n = self.shards.len();
        let start = self.next_claim_shard.fetch_add(1, Ordering::Relaxed);
        for i in 0..n {
            let shard = (start + i) % n;
            if let Some(job) = self.call(shard, |reply| Cmd::Claim { channel_id, lease_ms, reply })? {
                return Ok(Some(job));
            }
        }
        Ok(None)
    }

    /// Variante sans attente de `try_claim` : les shards sont parcourus par chaînage de callbacks.
    pub fn try_claim_async(
        self: &Arc<Self>,
        channel_id: u16,
        lease_ms: u32,
        done: impl FnOnce(Result<Option<ClaimedJob>>) + Send + 'static,
    ) {
        let start = self.next_claim_shard.fetch_add(1, Ordering::Relaxed);
        self.claim_step(channel_id, lease_ms, start, 0, Box::new(done));
    }

    /// Comme `try_claim_async`, mais en commençant par `start` (le balayage couvre toujours tous les shards).
    pub fn try_claim_async_from(
        self: &Arc<Self>,
        channel_id: u16,
        lease_ms: u32,
        start: usize,
        done: impl FnOnce(Result<Option<ClaimedJob>>) + Send + 'static,
    ) {
        self.claim_step(channel_id, lease_ms, start, 0, Box::new(done));
    }

    fn claim_step(
        self: &Arc<Self>,
        channel_id: u16,
        lease_ms: u32,
        start: usize,
        i: usize,
        done: Box<dyn FnOnce(Result<Option<ClaimedJob>>) + Send>,
    ) {
        let n = self.shards.len();
        if i >= n {
            return done(Ok(None));
        }
        let me = self.clone();
        self.call_async(
            (start + i) % n,
            |reply| Cmd::Claim { channel_id, lease_ms, reply },
            move |r| match r {
                Ok(None) => me.claim_step(channel_id, lease_ms, start, i + 1, done),
                other => done(other),
            },
        );
    }

    /// Réservation bloquante jusqu'à `wait_ms` ; réveillée par les notifications des shards.
    pub fn claim(&self, channel_id: u16, lease_ms: u32, wait_ms: u32) -> Result<Option<ClaimedJob>> {
        let deadline = Instant::now() + Duration::from_millis(wait_ms as u64);
        loop {
            let seen = self.notifier.generation();
            if let Some(job) = self.try_claim(channel_id, lease_ms)? {
                return Ok(Some(job));
            }
            if self.is_shutting_down() {
                return Err(shutting_down());
            }
            let Some(left) = deadline.checked_duration_since(Instant::now()).filter(|d| !d.is_zero()) else {
                return Ok(None);
            };
            self.notifier.wait_change(seen, left);
        }
    }

    /// Attend qu'un job soit disponible sur le canal, sans le réserver.
    pub fn wait_for_job(&self, channel_id: u16, timeout_ms: u32) -> Result<bool> {
        let deadline = Instant::now() + Duration::from_millis(timeout_ms as u64);
        loop {
            let seen = self.notifier.generation();
            if self.stats(Some(channel_id))?.ready > 0 {
                return Ok(true);
            }
            if self.is_shutting_down() {
                return Err(shutting_down());
            }
            let Some(left) = deadline.checked_duration_since(Instant::now()).filter(|d| !d.is_zero()) else {
                return Ok(false);
            };
            self.notifier.wait_change(seen, left);
        }
    }

    /// Le shard propriétaire est encodé dans les 16 bits de poids fort du `reservation_id`.
    fn reservation_shard(&self, r: &Reservation) -> Result<usize> {
        let shard = (r.reservation_id >> 48) as usize;
        if shard < self.shards.len() {
            Ok(shard)
        } else {
            Err(EngineError::new(ErrorCode::StaleReservation, "unknown reservation"))
        }
    }

    pub fn ack(&self, r: Reservation) -> Result<()> {
        let s = self.reservation_shard(&r)?;
        self.call(s, |reply| Cmd::Ack(r, reply))
    }

    pub fn ack_async(&self, r: Reservation, done: impl FnOnce(Result<()>) + Send + 'static) {
        match self.reservation_shard(&r) {
            Ok(s) => self.call_async(s, |reply| Cmd::Ack(r, reply), done),
            Err(e) => done(Err(e)),
        }
    }

    pub fn nack_async(&self, r: Reservation, kind: NackKind, done: impl FnOnce(Result<()>) + Send + 'static) {
        match self.reservation_shard(&r) {
            Ok(s) => self.call_async(s, |reply| Cmd::Nack(r, kind, reply), done),
            Err(e) => done(Err(e)),
        }
    }

    pub fn extend_lease_async(&self, r: Reservation, lease_ms: u32, done: impl FnOnce(Result<u64>) + Send + 'static) {
        match self.reservation_shard(&r) {
            Ok(s) => self.call_async(s, |reply| Cmd::Extend(r, lease_ms, reply), done),
            Err(e) => done(Err(e)),
        }
    }

    pub fn nack(&self, r: Reservation, kind: NackKind) -> Result<()> {
        let s = self.reservation_shard(&r)?;
        self.call(s, |reply| Cmd::Nack(r, kind, reply))
    }

    pub fn extend_lease(&self, r: Reservation, lease_ms: u32) -> Result<u64> {
        let s = self.reservation_shard(&r)?;
        self.call(s, |reply| Cmd::Extend(r, lease_ms, reply))
    }

    pub fn get(&self, job_id: u128) -> Result<JobView> {
        self.first_found(|i| self.call(i, |r| Cmd::Get(job_id, r)))
    }

    pub fn cancel(&self, job_id: u128) -> Result<()> {
        self.first_found(|i| self.call(i, |r| Cmd::Cancel(job_id, r)))
    }

    /// Le shard d'un job dépend de sa clé de partition : on interroge tous les shards.
    fn first_found<T>(&self, f: impl Fn(usize) -> Result<T>) -> Result<T> {
        for i in 0..self.shards.len() {
            match f(i) {
                Err(e) if e.code == ErrorCode::NotFound => continue,
                other => return other,
            }
        }
        Err(EngineError::new(ErrorCode::NotFound, "job not found"))
    }

    pub fn stats(&self, channel_id: Option<u16>) -> Result<Stats> {
        let mut total = Stats::default();
        for i in 0..self.shards.len() {
            total.add(&self.call(i, |r| Cmd::Stats(channel_id, r))?);
        }
        Ok(total)
    }

    fn generate_id(&self) -> u128 {
        let n = self.id_counter.fetch_add(1, Ordering::Relaxed) as u128;
        let t = now_ms() as u128;
        // Mélange (splitmix64) pour éviter des identifiants séquentiels prévisibles.
        let mix = |mut x: u64| {
            x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
            x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            x ^ (x >> 31)
        };
        let hi = mix((t as u64) ^ (n as u64).rotate_left(32));
        let lo = mix(hi ^ n as u64 ^ std::process::id() as u64);
        (((hi as u128) << 64) | lo as u128).max(1)
    }
}

fn shutting_down() -> EngineError {
    EngineError::new(ErrorCode::ShuttingDown, "engine is shutting down")
}

/// Réponse différée jusqu'à ce que les enregistrements WAL de la fournée soient durables.
pub(crate) type Pending = Vec<Box<dyn FnOnce(Option<&EngineError>) + Send>>;

/// Nombre maximal de commandes traitées avant un flush : borne la latence du group commit.
const MAX_BATCH: usize = 256;
/// Plafond de l'attente active : une configuration aberrante ne doit pas saturer un cœur.
/// Expérimental : une connexion commence ses claims par le shard de son dernier push.
pub(crate) fn claim_affinity() -> bool {
    static V: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *V.get_or_init(|| std::env::var_os("SOLID_ENGINE_CLAIM_AFFINITY").is_some())
}

pub(crate) const MAX_SPIN_US: u64 = 100;

/// Lit une durée de spin (µs) : valeur invalide → défaut, au-delà du plafond → plafond, avec avertissement.
pub(crate) fn parse_spin_us(name: &str, raw: Option<&str>, default_us: u64) -> u64 {
    match raw {
        None => default_us,
        Some(s) => match s.trim().parse::<u64>() {
            Ok(v) if v <= MAX_SPIN_US => v,
            Ok(v) => {
                eprintln!("{name}={v} dépasse {MAX_SPIN_US} µs : ramené à {MAX_SPIN_US}");
                MAX_SPIN_US
            }
            Err(_) => {
                eprintln!("{name}={s:?} invalide : valeur par défaut {default_us} µs");
                default_us
            }
        },
    }
}

fn env_spin_us(name: &str) -> u64 {
    parse_spin_us(name, std::env::var(name).ok().as_deref(), 0)
}

/// Durée d'attente active d'un shard avant de dormir (lue une seule fois).
fn shard_spin_us() -> u64 {
    static V: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *V.get_or_init(|| env_spin_us("SOLID_ENGINE_SPIN_SHARD_US"))
}

/// Idem pour l'écrivain de réponses d'une connexion.
/// Expérimental : attente maximale (µs) d'une trame supplémentaire avant d'écrire sur le réseau.
pub(crate) fn writer_linger_us() -> u64 {
    static V: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *V.get_or_init(|| env_spin_us("SOLID_ENGINE_WRITER_LINGER_US"))
}

pub(crate) fn writer_spin_us() -> u64 {
    static V: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *V.get_or_init(|| env_spin_us("SOLID_ENGINE_SPIN_WRITER_US"))
}

static SPIN_ATTEMPTS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static SPIN_HITS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static SPIN_SKIPPED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// (tentatives de spin, spins ayant trouvé du travail, spins évités par la porte adaptative).
pub fn spin_stats() -> (u64, u64, u64) {
    use std::sync::atomic::Ordering::Relaxed;
    (SPIN_ATTEMPTS.load(Relaxed), SPIN_HITS.load(Relaxed), SPIN_SKIPPED.load(Relaxed))
}

/// Exposant maximal du recul (0 = spin fixe, 2 = jusqu'à 3 attentes sautées, 6 = 63).
fn spin_backoff_cap() -> u32 {
    static V: std::sync::OnceLock<u32> = std::sync::OnceLock::new();
    *V.get_or_init(|| match std::env::var("SOLID_ENGINE_SPIN_BACKOFF") {
        Ok(s) => s.trim().parse::<u32>().map(|v| v.min(6)).unwrap_or_else(|_| {
            eprintln!("SOLID_ENGINE_SPIN_BACKOFF={s:?} invalide : valeur par défaut 2");
            2
        }),
        Err(_) => 2,
    })
}

/// Spin adaptatif : après un échec (le message n'est pas arrivé pendant le spin), on saute les
/// spins suivants avec un recul exponentiel plafonné ; un succès réarme le spin.
#[derive(Default)]
pub(crate) struct SpinGate {
    skip: u32,
    misses: u32,
}

impl SpinGate {
    /// Budget de spin (µs) pour la prochaine attente ; 0 = dormir directement.
    pub(crate) fn budget(&mut self, base_us: u64) -> u64 {
        use std::sync::atomic::Ordering::Relaxed;
        if base_us == 0 {
            0
        } else if self.skip > 0 {
            self.skip -= 1;
            SPIN_SKIPPED.fetch_add(1, Relaxed);
            0
        } else {
            SPIN_ATTEMPTS.fetch_add(1, Relaxed);
            base_us
        }
    }

    pub(crate) fn record(&mut self, hit: bool) {
        if hit {
            SPIN_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.misses = 0;
            self.skip = 0;
        } else {
            self.misses = (self.misses + 1).min(spin_backoff_cap());
            self.skip = (1 << self.misses) - 1;
        }
    }
}

fn defer<T: Send + 'static>(pending: &mut Pending, tx: Reply<T>, result: Result<T>) {
    pending.push(Box::new(move |failure| {
        let _ = tx.send(match failure {
            Some(e) => Err(e.clone()),
            None => result,
        });
    }));
}

fn reply_read<T>(tx: Reply<T>, failed: &Option<EngineError>, result: impl FnOnce() -> Result<T>) {
    let _ = tx.send(match failed {
        Some(e) => Err(e.clone()),
        None => result(),
    });
}

enum WalMsg {
    Append(Vec<solid_engine_core::record::Record>),
    /// `strict` : les réponses sont libérées par le writer une fois la fournée durable.
    Commit(Vec<solid_engine_core::record::Record>, Pending),
    Snapshot(Vec<u8>, std::sync::mpsc::SyncSender<std::io::Result<()>>),
}

/// Modes `balanced` et `strict` : un thread unique possède le WAL (encodage, écriture, fsync).
/// En `balanced` le shard répond sans attendre le disque ; en `strict` les réponses sont
/// transmises au writer, qui les libère après un fsync commun à toutes les soumissions en attente.
/// Le shard n'attend jamais le disque, et l'écriture et le fsync concurrents ne le ralentissent plus.
/// Une erreur d'I/O est publiée dans `error` et fait passer le shard en échec.
struct WalWriter {
    tx: Option<std::sync::mpsc::SyncSender<WalMsg>>,
    error: Arc<std::sync::Mutex<Option<String>>>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl WalWriter {
    fn spawn(shard_id: usize, mut wal: Wal, sync_interval: Duration) -> Self {
        // File bornée : si le disque prend du retard, le shard est ralenti plutôt que la mémoire saturée.
        let (tx, rx) = std::sync::mpsc::sync_channel::<WalMsg>(65_536);
        let error = Arc::new(std::sync::Mutex::new(None::<String>));
        let slot = error.clone();
        let handle = std::thread::Builder::new()
            .name(format!("solid-engine-wal-{shard_id}"))
            .spawn(move || {
                let fail = |what: &str, e: std::io::Error| {
                    slot.lock().unwrap().get_or_insert(format!("{what}: {e}"));
                };
                let mut due: Option<Instant> = None;
                let mut carry: Option<WalMsg> = None;
                let mut tally = crate::stats::Tally::default().with_cpu_role(crate::stats::C::CpuWalNs);
                loop {
                    let msg = match carry.take() {
                        Some(m) => Ok(m),
                        None => match due {
                            Some(t) => rx.recv_timeout(t.saturating_duration_since(Instant::now())),
                            None => rx.recv().map_err(|_| RecvTimeoutError::Disconnected),
                        },
                    };
                    if msg.is_ok() {
                        tally.add(crate::stats::C::WalMsgs, 1);
                        tally.tick();
                    }
                    match msg {
                        Ok(WalMsg::Append(records)) => {
                            if slot.lock().unwrap().is_none() {
                                match wal.append(&records) {
                                    Ok(()) => due = due.or(Some(Instant::now() + sync_interval)),
                                    Err(e) => fail("WAL write failed", e),
                                }
                            }
                        }
                        Ok(WalMsg::Commit(records, pending)) => {
                            let mut batch = vec![pending];
                            let mut write = |records: Vec<solid_engine_core::record::Record>| {
                                if slot.lock().unwrap().is_none() && !records.is_empty() {
                                    if let Err(e) = wal.append(&records) {
                                        fail("WAL write failed", e);
                                    }
                                }
                            };
                            write(records);
                            // Group commit : on absorbe tout ce qui est déjà en file avant le fsync.
                            while batch.len() < 1024 {
                                match rx.try_recv() {
                                    Ok(WalMsg::Commit(r, p)) => {
                                        write(r);
                                        batch.push(p);
                                    }
                                    Ok(other) => {
                                        carry = Some(other);
                                        break;
                                    }
                                    Err(_) => break,
                                }
                            }
                            if slot.lock().unwrap().is_none() {
                                if let Err(e) = wal.sync() {
                                    fail("WAL sync failed", e);
                                }
                            }
                            let failure = slot.lock().unwrap().clone().map(|e| io_failure("WAL failure", e));
                            for p in batch.into_iter().flatten() {
                                p(failure.as_ref());
                            }
                        }
                        Ok(WalMsg::Snapshot(state, reply)) => {
                            let result = wal.snapshot(&state);
                            if result.is_ok() {
                                due = None;
                            }
                            let _ = reply.send(result);
                        }
                        Err(RecvTimeoutError::Timeout) => {
                            due = None;
                            if let Err(e) = wal.sync() {
                                fail("WAL sync failed", e);
                            }
                        }
                        Err(RecvTimeoutError::Disconnected) => {
                            if let Err(e) = wal.sync() {
                                fail("WAL sync failed", e);
                            }
                            return;
                        }
                    }
                }
            })
            .expect("spawn WAL writer");
        Self { tx: Some(tx), error, handle: Some(handle) }
    }

    fn take_error(&self) -> Option<String> {
        self.error.lock().unwrap().clone()
    }

    /// Arrêt propre : ferme la file, le thread synchronise puis se termine.
    fn finish(mut self) {
        self.tx = None;
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

pub(crate) struct Durable {
    /// WAL exploité directement par le shard (mode `strict`).
    wal: Option<Wal>,
    /// WAL exploité par un thread dédié (mode `balanced`).
    writer: Option<WalWriter>,
    mode: Durability,
    since_snapshot: u64,
}

fn open_shard(id: usize, cfg: &Arc<Config>) -> std::result::Result<(Shard, Option<Durable>), String> {
    let storage = &cfg.storage;
    if storage.durability == Durability::Memory {
        return Ok((Shard::new(id as u16, cfg.clone()), None));
    }
    let dir = Path::new(storage.data_dir.as_deref().unwrap()).join(format!("shard-{id}"));
    let started = Instant::now();
    let (shard, wal) = recover_shard(&dir, id as u16, cfg.clone()).map_err(|e| format!("shard {id}: {e}"))?;
    eprintln!("shard {id} recovered in {:?} (last sequence {})", started.elapsed(), wal.last_seq());
    let since_snapshot = wal.records_since_snapshot();
    let mode = storage.durability;
    let (wal, writer) = if mode != Durability::Memory {
        (None, Some(WalWriter::spawn(id, wal, Duration::from_millis(storage.sync_interval_ms))))
    } else {
        (Some(wal), None)
    };
    Ok((shard, Some(Durable { wal, writer, mode, since_snapshot })))
}

pub(crate) fn close_durable(d: Durable) {
    if let Some(mut wal) = d.wal {
        let _ = wal.sync();
    }
    if let Some(w) = d.writer {
        w.finish();
    }
}

fn io_failure(what: &str, e: impl std::fmt::Display) -> EngineError {
    EngineError::new(ErrorCode::Internal, format!("{what}: {e}"))
}

/// Écrit les enregistrements de la fournée, synchronise selon le mode, puis libère les réponses.
/// Une erreur d'I/O place le shard en échec : plus aucun succès n'est confirmé.
pub(crate) fn flush(
    shard: &mut Shard,
    durable: &mut Option<Durable>,
    failed: &mut Option<EngineError>,
    pending: &mut Pending,
    cfg: &Config,
) {
    let records = shard.take_log();
    if let Some(d) = durable.as_mut() {
        if failed.is_none() {
            let count = records.len() as u64;
            if let Some(w) = &d.writer {
                if let Some(e) = w.take_error() {
                    *failed = Some(io_failure("WAL failure", e));
                } else if d.mode == Durability::Strict {
                    if count > 0 {
                        // Les réponses partent avec les enregistrements : le writer les libère après le fsync.
                        let batch = std::mem::take(pending);
                        let sent = w.tx.as_ref().is_some_and(|tx| tx.send(WalMsg::Commit(records, batch)).is_ok());
                        if !sent {
                            *failed = Some(io_failure("WAL writer", "stopped"));
                        }
                    }
                } else if count > 0 {
                    let sent = w.tx.as_ref().is_some_and(|tx| tx.send(WalMsg::Append(records)).is_ok());
                    if !sent {
                        *failed = Some(io_failure("WAL writer", "stopped"));
                    }
                }
            } else if let Some(wal) = d.wal.as_mut() {
                if count > 0 {
                    let mut r = wal.append(&records);
                    if d.mode == Durability::Strict && r.is_ok() {
                        r = wal.sync();
                    }
                    if let Err(e) = r {
                        *failed = Some(io_failure("WAL write failed", e));
                    }
                }
            }
            d.since_snapshot += count;
        }
        let every = cfg.storage.snapshot_every_records;
        if failed.is_none() && every > 0 && d.since_snapshot >= every {
            let state = shard.encode_snapshot();
            let result = if let Some(w) = &d.writer {
                let (reply, done) = std::sync::mpsc::sync_channel(1);
                match w.tx.as_ref().map(|tx| tx.send(WalMsg::Snapshot(state, reply))) {
                    Some(Ok(())) => done.recv().unwrap_or_else(|_| Err(std::io::Error::other("WAL writer stopped"))),
                    _ => Err(std::io::Error::other("WAL writer stopped")),
                }
            } else {
                d.wal.as_mut().unwrap().snapshot(&state)
            };
            match result {
                Ok(()) => d.since_snapshot = 0,
                Err(e) => *failed = Some(io_failure("snapshot failed", e)),
            }
        }
        if failed.is_none() {
            if let Some(e) = d.writer.as_ref().and_then(WalWriter::take_error) {
                *failed = Some(io_failure("WAL failure", e));
            }
        }
    }
    for p in pending.drain(..) {
        p(failed.as_ref());
    }
}

fn shard_thread(
    id: usize,
    cfg: Arc<Config>,
    rx: Receiver<Cmd>,
    notifier: Arc<Notifier>,
    ready: std::sync::mpsc::Sender<std::result::Result<(), String>>,
) {
    let mut gate = SpinGate::default();
    let (mut shard, mut durable) = match open_shard(id, &cfg) {
        Ok(v) => {
            let _ = ready.send(Ok(()));
            v
        }
        Err(e) => {
            let _ = ready.send(Err(e));
            return;
        }
    };
    shard.set_logging(durable.is_some());
    let mut failed: Option<EngineError> = None;
    let mut pending = Pending::new();
    let mem_stats = std::env::var("SOLID_ENGINE_MEM_STATS").is_ok();
    let mut last_stats = Instant::now();
    let mut tally = crate::stats::Tally::default().with_cpu_role(crate::stats::C::CpuShardNs);

    loop {
        let now = now_ms();
        shard.tick(now);
        if mem_stats && last_stats.elapsed() >= Duration::from_secs(5) {
            last_stats = Instant::now();
            let [jobs, cap, ret, leases, sched, ready] = shard.sizes();
            eprintln!("mem_stats shard={id} jobs={jobs} cap={cap} retention={ret} leases={leases} scheduled={sched} ready={ready}");
        }
        flush(&mut shard, &mut durable, &mut failed, &mut pending, &cfg);
        if shard.take_ready_flag() {
            notifier.notify();
        }

        let wait = shard.next_deadline().map(|t| Duration::from_millis(t.saturating_sub(now)));
        let wait = if mem_stats { Some(wait.map_or(Duration::from_secs(5), |w| w.min(Duration::from_secs(5)))) } else { wait };
        // Courte attente active avant de dormir : évite un réveil (appel système) par commande.
        let budget = gate.budget(shard_spin_us());
        let spin_until = Instant::now() + Duration::from_micros(budget);
        let mut spun = None;
        while Instant::now() < spin_until {
            match rx.try_recv() {
                Ok(c) => {
                    spun = Some(c);
                    break;
                }
                Err(std::sync::mpsc::TryRecvError::Disconnected) => return,
                Err(_) => std::hint::spin_loop(),
            }
        }
        if budget > 0 {
            gate.record(spun.is_some());
        }
        let first = match (spun, wait) {
            (Some(c), _) => Some(c),
            (None, wait) => {
                tally.add(crate::stats::C::ShardBlockWaits, 1);
                tally.flush();
                let got = match wait {
                    Some(w) => match rx.recv_timeout(w) {
                        Ok(c) => Some(c),
                        Err(RecvTimeoutError::Timeout) => None,
                        Err(RecvTimeoutError::Disconnected) => return,
                    },
                    None => match rx.recv() {
                        Ok(c) => Some(c),
                        Err(_) => return,
                    },
                };
                if got.is_some() {
                    tally.add(crate::stats::C::ShardWakeups, 1);
                }
                got
            }
        };

        let mut stop = false;
        let mut next = first;
        let mut handled = 0;
        while let Some(cmd) = next {
            let now = now_ms();
            match cmd {
                Cmd::Push(r, tx) => defer(&mut pending, tx, shard.push(r, now)),
                Cmd::Claim { channel_id, lease_ms, reply } => {
                    defer(&mut pending, reply, shard.claim(channel_id, lease_ms, now))
                }
                Cmd::Ack(r, tx) => defer(&mut pending, tx, shard.ack(r, now)),
                Cmd::Nack(r, k, tx) => defer(&mut pending, tx, shard.nack(r, k, now)),
                Cmd::Extend(r, ms, tx) => defer(&mut pending, tx, shard.extend_lease(r, ms, now)),
                Cmd::Cancel(id, tx) => defer(&mut pending, tx, shard.cancel(id, now)),
                Cmd::Get(id, tx) => reply_read(tx, &failed, || shard.get(id, now)),
                Cmd::Stats(ch, tx) => reply_read(tx, &failed, || Ok(shard.stats(ch, now))),
                Cmd::Shutdown => {
                    stop = true;
                    break;
                }
            }
            handled += 1;
            next = if handled < MAX_BATCH { rx.try_recv().ok() } else { None };
        }
        if handled > 0 {
            tally.add(crate::stats::C::ShardCommands, handled as u64);
            tally.add(crate::stats::C::ShardBatches, 1);
            tally.tick();
        }
        flush(&mut shard, &mut durable, &mut failed, &mut pending, &cfg);
        if stop {
            if let Some(d) = durable.take() {
                if let Some(mut wal) = d.wal {
                    let _ = wal.sync();
                }
                if let Some(w) = d.writer {
                    w.finish();
                }
            }
            return;
        }
        if shard.take_ready_flag() {
            notifier.notify();
        }
    }
}

#[cfg(test)]
mod spin_tests {
    use super::*;

    #[test]
    fn spin_value_is_validated_and_capped() {
        assert_eq!(parse_spin_us("X", None, 0), 0);
        assert_eq!(parse_spin_us("X", Some("30"), 0), 30);
        assert_eq!(parse_spin_us("X", Some("5000"), 0), MAX_SPIN_US);
        assert_eq!(parse_spin_us("X", Some("abc"), 0), 0);
        assert_eq!(parse_spin_us("X", Some("-1"), 0), 0);
    }

    #[test]
    fn spin_gate_backs_off_after_misses_and_rearms_on_hit() {
        let mut g = SpinGate::default();
        assert_eq!(g.budget(30), 30);
        g.record(false);
        assert_eq!((g.budget(30), g.budget(30)), (0, 30));
        g.record(false);
        assert_eq!([g.budget(30), g.budget(30), g.budget(30)], [0, 0, 0]);
        g.record(true);
        assert_eq!(g.budget(30), 30);
    }
}
