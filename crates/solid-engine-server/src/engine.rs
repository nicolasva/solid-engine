//! Runtime du moteur : un thread par shard (shared-nothing), mailboxes bornées,
//! routage des commandes et réveil événementiel des workers en attente.

use solid_engine_core::config::{Config, Durability};
use solid_engine_core::shard::Shard;
use solid_engine_core::*;
use solid_engine_storage::{lock_data_dir, recover_shard, DataDirLock, Wal};
use std::path::Path;
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
    PushMany(Vec<PushRequest>, Reply<Vec<Result<PushOutcome>>>),
    Claim {
        channel_id: u16,
        lease_ms: u32,
        reply: Reply<Option<ClaimedJob>>,
    },
    ClaimMany {
        channel_id: u16,
        lease_ms: u32,
        max: usize,
        reply: Reply<Vec<ClaimedJob>>,
    },
    /// Comme `ClaimMany`, mais livre des trames `Dispatch` déjà encodées (payload copié une seule fois).
    ClaimDispatch {
        channel_id: u16,
        lease_ms: u32,
        max: usize,
        sent: Instant,
        reply: Reply<Vec<solid_engine_protocol::DispatchBody>>,
    },
    AckMany(Vec<Reservation>, Reply<Vec<Option<EngineError>>>),
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
    /// Même compteur, lisible sans verrou (réacteurs Hyperlane).
    ticks: AtomicU64,
    /// Réacteurs à réveiller par eventfd : (drapeau « affamé », eventfd).
    wakers: std::sync::RwLock<Vec<(Arc<AtomicBool>, i32)>>,
}

impl Notifier {
    fn notify(&self) {
        *self.generation.lock().unwrap() += 1;
        self.ticks.fetch_add(1, Ordering::Release);
        self.cv.notify_all();
        for (hungry, fd) in self.wakers.read().unwrap().iter() {
            if hungry.swap(false, Ordering::AcqRel) {
                extern "C" {
                    fn write(fd: i32, buf: *const std::ffi::c_void, n: usize) -> isize;
                }
                let one = 1u64.to_ne_bytes();
                unsafe { write(*fd, one.as_ptr().cast(), 8) };
            }
        }
    }

    fn generation(&self) -> u64 {
        *self.generation.lock().unwrap()
    }

    fn wait_change(&self, seen: u64, timeout: Duration) {
        let g = self.generation.lock().unwrap();
        let _ = self
            .cv
            .wait_timeout_while(g, timeout, |g| *g == seen)
            .unwrap();
    }
}

/// Boîte aux lettres d'un shard possédé par un réacteur (mode direct) : le réacteur la vide à chaque tour.
pub(crate) struct Mailbox {
    queue: Mutex<Vec<Cmd>>,
    capacity: usize,
    wake_fd: std::sync::atomic::AtomicI32,
}

impl Mailbox {
    fn new(capacity: usize) -> Self {
        Self {
            queue: Mutex::new(Vec::new()),
            capacity,
            wake_fd: std::sync::atomic::AtomicI32::new(-1),
        }
    }

    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    pub(crate) fn set_wake_fd(&self, fd: i32) {
        self.wake_fd.store(fd, Ordering::SeqCst);
    }

    fn push(&self, cmd: Cmd) -> std::result::Result<(), TrySendError<Cmd>> {
        let mut q = self.queue.lock().unwrap();
        if q.len() >= self.capacity {
            return Err(TrySendError::Full(cmd));
        }
        let was_empty = q.is_empty();
        q.push(cmd);
        drop(q);
        let fd = self.wake_fd.load(Ordering::SeqCst);
        if was_empty && fd >= 0 {
            extern "C" {
                fn write(fd: i32, buf: *const std::ffi::c_void, n: usize) -> isize;
            }
            let one = 1u64.to_ne_bytes();
            unsafe { write(fd, one.as_ptr().cast(), 8) };
        }
        Ok(())
    }

    /// Exécute les commandes reçues sur le shard ; renvoie vrai si un arrêt a été demandé.
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    pub(crate) fn apply(
        &self,
        shard: &mut Shard,
        pending: &mut Pending,
        failed: &Option<EngineError>,
        now: u64,
    ) -> bool {
        let cmds = std::mem::take(&mut *self.queue.lock().unwrap());
        cmds.into_iter().fold(false, |stop, cmd| {
            apply_cmd(shard, pending, failed, cmd, now) || stop
        })
    }
}

enum ShardTx {
    Channel(SyncSender<Cmd>),
    Mailbox(Arc<Mailbox>),
}

impl ShardTx {
    fn try_send(&self, cmd: Cmd) -> std::result::Result<(), TrySendError<Cmd>> {
        match self {
            ShardTx::Channel(tx) => tx.try_send(cmd),
            ShardTx::Mailbox(m) => m.push(cmd),
        }
    }
}

pub struct Engine {
    cfg: Arc<Config>,
    shards: Vec<ShardTx>,
    threads: Mutex<Vec<JoinHandle<()>>>,
    notifier: Arc<Notifier>,
    next_claim_shard: AtomicUsize,
    id_counter: AtomicU64,
    shutting_down: AtomicBool,
    lock: Mutex<Option<DataDirLock>>,
    /// Prototype Zero-Dispatch : shard 0 (et son WAL) confié au réacteur.
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    direct: Mutex<Vec<Option<(Shard, Option<Durable>, Arc<Mailbox>)>>>,
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
        let mut direct = Vec::new();
        let spawned = if direct_enabled() {
            for id in 0..cfg.engine.shards {
                let (shard, durable) = open_shard(id, &cfg)?;
                let mailbox = Arc::new(Mailbox::new(cfg.engine.mailbox_capacity));
                senders.push(ShardTx::Mailbox(mailbox.clone()));
                direct.push(Some((shard, durable, mailbox)));
            }
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
            senders.push(ShardTx::Channel(tx));
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

    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    pub(crate) fn take_direct(&self, id: usize) -> Option<(Shard, Option<Durable>, Arc<Mailbox>)> {
        self.direct.lock().unwrap().get_mut(id)?.take()
    }

    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    pub(crate) fn shard_count(&self) -> usize {
        self.shards.len()
    }

    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    pub(crate) fn notify_ready(&self) {
        self.notifier.notify();
    }

    /// Un réacteur « affamé » (abonnés sans job local) est réveillé quand un job devient prêt ailleurs.
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    pub(crate) fn register_waker(&self, hungry: Arc<AtomicBool>, fd: i32) {
        self.notifier.wakers.write().unwrap().push((hungry, fd));
    }

    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    pub(crate) fn notify_ticks(&self) -> u64 {
        self.notifier.ticks.load(Ordering::Acquire)
    }

    pub fn config(&self) -> &Config {
        &self.cfg
    }

    pub fn shutdown(&self) {
        self.shutting_down.store(true, Ordering::SeqCst);
        for s in &self.shards {
            let _ = s.try_send(Cmd::Shutdown);
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

    fn call<T: Send + 'static>(
        &self,
        shard: usize,
        make: impl FnOnce(Reply<T>) -> Cmd,
    ) -> Result<T> {
        let (tx, rx) = sync_channel(1);
        let reply = Reply::new(move |r| {
            let _ = tx.send(r);
        });
        match self.shards[shard].try_send(make(reply)) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                return Err(EngineError::new(
                    ErrorCode::Overloaded,
                    "shard mailbox is full",
                ))
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
            Err(TrySendError::Full(_)) => {
                EngineError::new(ErrorCode::Overloaded, "shard mailbox is full")
            }
            Err(TrySendError::Disconnected(_)) => shutting_down(),
        };
        let done = slot.lock().unwrap().take();
        if let Some(done) = done {
            done(Err(error));
        }
    }

    pub(crate) fn push_shard(&self, req: &mut PushRequest) -> usize {
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
    pub fn push_async(
        &self,
        mut req: PushRequest,
        done: impl FnOnce(Result<PushOutcome>) + Send + 'static,
    ) -> usize {
        let shard = self.push_shard(&mut req);
        self.call_async(shard, |r| Cmd::Push(req, r), done);
        shard
    }

    /// Publication groupée : une commande par shard concerné (routage identique à `push`), résultats
    /// individuels dans l'ordre d'entrée. Chaque groupe n'est libéré qu'une fois ses jobs durables ;
    /// en cas d'échec d'un shard, seuls ses jobs sont en erreur (succès partiel possible, rejeu idempotent
    /// grâce au `job_id`).
    pub fn push_many_async(
        &self,
        reqs: Vec<PushRequest>,
        done: impl FnOnce(Vec<Result<PushOutcome>>) + Send + 'static,
    ) {
        struct Gather {
            results: Vec<Option<Result<PushOutcome>>>,
            remaining: usize,
            done: Option<Box<dyn FnOnce(Vec<Result<PushOutcome>>) + Send>>,
        }
        let n = reqs.len();
        if n == 0 {
            return done(Vec::new());
        }
        let mut groups: Vec<(Vec<usize>, Vec<PushRequest>)> =
            (0..self.shards.len()).map(|_| Default::default()).collect();
        for (i, mut req) in reqs.into_iter().enumerate() {
            let shard = self.push_shard(&mut req);
            groups[shard].0.push(i);
            groups[shard].1.push(req);
        }
        let used = groups.iter().filter(|g| !g.1.is_empty()).count();
        let gather = Arc::new(Mutex::new(Gather {
            results: (0..n).map(|_| None).collect(),
            remaining: used,
            done: Some(Box::new(done)),
        }));
        for (shard, (idx, rs)) in groups.into_iter().enumerate() {
            if rs.is_empty() {
                continue;
            }
            let gather = gather.clone();
            self.call_async(
                shard,
                |reply| Cmd::PushMany(rs, reply),
                move |res| {
                    let finished = {
                        let mut g = gather.lock().unwrap();
                        match res {
                            Ok(per_job) => {
                                for (i, r) in idx.into_iter().zip(per_job) {
                                    g.results[i] = Some(r);
                                }
                            }
                            Err(e) => {
                                for i in idx {
                                    g.results[i] = Some(Err(e.clone()));
                                }
                            }
                        }
                        g.remaining -= 1;
                        if g.remaining == 0 {
                            let results = std::mem::take(&mut g.results);
                            g.done.take().map(|d| (d, results))
                        } else {
                            None
                        }
                    };
                    if let Some((done, results)) = finished {
                        done(
                            results
                                .into_iter()
                                .map(|r| r.unwrap_or_else(|| Err(shutting_down())))
                                .collect(),
                        );
                    }
                },
            );
        }
    }

    pub fn push_many(&self, reqs: Vec<PushRequest>) -> Vec<Result<PushOutcome>> {
        let (tx, rx) = std::sync::mpsc::channel();
        self.push_many_async(reqs, move |r| {
            let _ = tx.send(r);
        });
        rx.recv().unwrap_or_default()
    }

    /// Un essai de réservation sur tous les shards, en commençant par un shard tournant.
    pub fn try_claim(&self, channel_id: u16, lease_ms: u32) -> Result<Option<ClaimedJob>> {
        let n = self.shards.len();
        let start = self.next_claim_shard.fetch_add(1, Ordering::Relaxed);
        for i in 0..n {
            let shard = (start + i) % n;
            if let Some(job) = self.call(shard, |reply| Cmd::Claim {
                channel_id,
                lease_ms,
                reply,
            })? {
                return Ok(Some(job));
            }
        }
        Ok(None)
    }

    /// Réserve jusqu'à `max` jobs sans attendre, avec une seule commande par shard visité.
    pub fn try_claim_many(
        &self,
        channel_id: u16,
        lease_ms: u32,
        max: usize,
    ) -> Result<Vec<ClaimedJob>> {
        let n = self.shards.len();
        let start = self.next_claim_shard.fetch_add(1, Ordering::Relaxed);
        let mut jobs = Vec::new();
        for i in 0..n {
            if jobs.len() >= max {
                break;
            }
            let want = max - jobs.len();
            match self.call((start + i) % n, |reply| Cmd::ClaimMany {
                channel_id,
                lease_ms,
                max: want,
                reply,
            }) {
                Ok(mut got) => jobs.append(&mut got),
                Err(_) if !jobs.is_empty() => break,
                Err(e) => return Err(e),
            }
        }
        Ok(jobs)
    }

    /// Réserve jusqu'à `max` jobs, déjà encodés en trames `Dispatch` bornées en octets.
    pub fn try_claim_dispatch(
        &self,
        channel_id: u16,
        lease_ms: u32,
        max: usize,
    ) -> Result<Vec<solid_engine_protocol::DispatchBody>> {
        let n = self.shards.len();
        let start = self.next_claim_shard.fetch_add(1, Ordering::Relaxed);
        let mut bodies: Vec<solid_engine_protocol::DispatchBody> = Vec::new();
        let mut got = 0;
        for i in 0..n {
            if got >= max {
                break;
            }
            match self.call((start + i) % n, |reply| Cmd::ClaimDispatch {
                channel_id,
                lease_ms,
                max: max - got,
                sent: Instant::now(),
                reply,
            }) {
                Ok(b) => {
                    got += b.iter().map(|x| x.count()).sum::<usize>();
                    bodies.extend(b);
                }
                Err(_) if !bodies.is_empty() => break,
                Err(e) => return Err(e),
            }
        }
        Ok(bodies)
    }

    /// Comme `try_claim_dispatch`, mais sollicite tous les shards en parallèle (quota équitable chacun,
    /// somme bornée par `max`), puis complète séquentiellement si un shard a rempli son quota.
    pub fn try_claim_dispatch_fanout(
        &self,
        channel_id: u16,
        lease_ms: u32,
        max: usize,
    ) -> Result<Vec<solid_engine_protocol::DispatchBody>> {
        let n = self.shards.len();
        if n == 1 || max < n * 2 {
            return self.try_claim_dispatch(channel_id, lease_ms, max);
        }
        let quota = max.div_ceil(n);
        let start = self.next_claim_shard.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = std::sync::mpsc::channel();
        for i in 0..n {
            let (tx, shard) = (tx.clone(), (start + i) % n);
            let want = quota.min(max.saturating_sub(quota * i));
            if want == 0 {
                continue;
            }
            self.call_async(
                shard,
                |reply| Cmd::ClaimDispatch {
                    channel_id,
                    lease_ms,
                    max: want,
                    sent: Instant::now(),
                    reply,
                },
                move |r| {
                    let _ = tx.send((want, r));
                },
            );
        }
        drop(tx);
        let (mut bodies, mut got, mut full, mut error) = (Vec::new(), 0usize, false, None);
        for (want, r) in rx {
            match r {
                Ok(b) => {
                    let c: usize = b.iter().map(|x| x.count()).sum();
                    full |= c == want;
                    got += c;
                    bodies.extend(b);
                }
                Err(e) => error = Some(e),
            }
        }
        if bodies.is_empty() {
            return error.map_or(Ok(bodies), Err);
        }
        if full && got < max {
            if let Ok(more) = self.try_claim_dispatch(channel_id, lease_ms, max - got) {
                bodies.extend(more);
            }
        }
        Ok(bodies)
    }

    /// Variante bloquante de `try_claim_dispatch` : attend un job jusqu'à `wait_ms`.
    pub fn claim_dispatch_stream(
        &self,
        channel_id: u16,
        lease_ms: u32,
        max: usize,
        wait_ms: u32,
    ) -> Result<Vec<solid_engine_protocol::DispatchBody>> {
        let deadline = Instant::now() + Duration::from_millis(wait_ms as u64);
        loop {
            let seen = self.notifier.generation();
            static FANOUT: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
            let fanout = *FANOUT.get_or_init(|| std::env::var_os("SOLID_ENGINE_CLAIM_FANOUT").is_some());
            let claimed = if fanout {
                self.try_claim_dispatch_fanout(channel_id, lease_ms, max)
            } else {
                self.try_claim_dispatch(channel_id, lease_ms, max)
            };
            match claimed {
                Ok(b) if !b.is_empty() => return Ok(b),
                Ok(_) => {}
                Err(e) if e.code == ErrorCode::Overloaded => std::thread::yield_now(),
                Err(e) => return Err(e),
            }
            if self.is_shutting_down() {
                return Err(shutting_down());
            }
            let Some(left) = deadline
                .checked_duration_since(Instant::now())
                .filter(|d| !d.is_zero())
            else {
                return Ok(Vec::new());
            };
            self.notifier.wait_change(seen, left);
        }
    }

    /// Hyperlane : réserve jusqu'à `max` jobs en sollicitant tous les shards en parallèle (un quota
    /// équitable chacun), puis complète séquentiellement si des shards ont rempli leur quota.
    pub fn try_claim_fanout(
        &self,
        channel_id: u16,
        lease_ms: u32,
        max: usize,
    ) -> Result<Vec<ClaimedJob>> {
        let n = self.shards.len();
        if n == 1 || max < n * 2 {
            return self.try_claim_many(channel_id, lease_ms, max);
        }
        let quota = max.div_ceil(n);
        let start = self.next_claim_shard.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = std::sync::mpsc::channel();
        for i in 0..n {
            let (tx, shard) = (tx.clone(), (start + i) % n);
            let want = quota.min(max.saturating_sub(quota * i));
            if want == 0 {
                continue;
            }
            self.call_async(
                shard,
                |reply| Cmd::ClaimMany {
                    channel_id,
                    lease_ms,
                    max: want,
                    reply,
                },
                move |r| {
                    let _ = tx.send((want, r));
                },
            );
        }
        drop(tx);
        let (mut jobs, mut full, mut error) = (Vec::with_capacity(max), false, None);
        for (want, r) in rx {
            match r {
                Ok(mut got) => {
                    full |= got.len() == want;
                    jobs.append(&mut got);
                }
                Err(e) => error = Some(e),
            }
        }
        if jobs.is_empty() {
            return error.map_or(Ok(jobs), Err);
        }
        if full && jobs.len() < max {
            if let Ok(mut more) = self.try_claim_many(channel_id, lease_ms, max - jobs.len()) {
                jobs.append(&mut more);
            }
        }
        Ok(jobs)
    }

    /// Hyperlane : attend (sans scrutation) qu'au moins un job soit prêt, puis en réserve jusqu'à `max`.
    pub fn claim_stream(
        &self,
        channel_id: u16,
        lease_ms: u32,
        max: usize,
        wait_ms: u32,
    ) -> Result<Vec<ClaimedJob>> {
        let deadline = Instant::now() + Duration::from_millis(wait_ms as u64);
        loop {
            let seen = self.notifier.generation();
            let claimed = if std::env::var_os("SOLID_ENGINE_HYPER_FANOUT").is_some() {
                self.try_claim_fanout(channel_id, lease_ms, max)
            } else {
                self.try_claim_many(channel_id, lease_ms, max)
            };
            match claimed {
                Ok(jobs) if !jobs.is_empty() => return Ok(jobs),
                Ok(_) => {}
                Err(e) if e.code == ErrorCode::Overloaded => std::thread::yield_now(),
                Err(e) => return Err(e),
            }
            if self.is_shutting_down() {
                return Err(shutting_down());
            }
            let Some(left) = deadline
                .checked_duration_since(Instant::now())
                .filter(|d| !d.is_zero())
            else {
                return Ok(Vec::new());
            };
            self.notifier.wait_change(seen, left);
        }
    }

    /// Hyperlane (réacteurs) : réserve jusqu'à `max` jobs sur les shards autres que `skip`, sans bloquer.
    /// Les shards sont visités l'un après l'autre par chaînage de callbacks.
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    pub(crate) fn claim_many_async(
        self: &Arc<Self>,
        channel_id: u16,
        lease_ms: u32,
        max: usize,
        skip: usize,
        done: impl FnOnce(Result<Vec<ClaimedJob>>) + Send + 'static,
    ) {
        let start = self.next_claim_shard.fetch_add(1, Ordering::Relaxed);
        self.claim_many_step(
            ClaimManyArgs {
                channel_id,
                lease_ms,
                max,
                skip,
                start,
            },
            0,
            Vec::new(),
            Box::new(done),
        );
    }

    fn claim_many_step(
        self: &Arc<Self>,
        a: ClaimManyArgs,
        mut i: usize,
        acc: Vec<ClaimedJob>,
        done: Box<dyn FnOnce(Result<Vec<ClaimedJob>>) + Send>,
    ) {
        let n = self.shards.len();
        while i < n && (a.start + i) % n == a.skip {
            i += 1;
        }
        if i >= n || acc.len() >= a.max {
            return done(Ok(acc));
        }
        let me = self.clone();
        let want = a.max - acc.len();
        self.call_async(
            (a.start + i) % n,
            |reply| Cmd::ClaimMany {
                channel_id: a.channel_id,
                lease_ms: a.lease_ms,
                max: want,
                reply,
            },
            move |r| {
                let mut acc = acc;
                match r {
                    Ok(mut got) => {
                        acc.append(&mut got);
                        me.claim_many_step(a, i + 1, acc, done)
                    }
                    Err(e) if e.code == ErrorCode::Overloaded => {
                        me.claim_many_step(a, i + 1, acc, done)
                    }
                    Err(e) if acc.is_empty() => done(Err(e)),
                    Err(_) => done(Ok(acc)),
                }
            },
        );
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
            |reply| Cmd::Claim {
                channel_id,
                lease_ms,
                reply,
            },
            move |r| match r {
                Ok(None) => me.claim_step(channel_id, lease_ms, start, i + 1, done),
                other => done(other),
            },
        );
    }

    /// Réservation bloquante jusqu'à `wait_ms` ; réveillée par les notifications des shards.
    pub fn claim(
        &self,
        channel_id: u16,
        lease_ms: u32,
        wait_ms: u32,
    ) -> Result<Option<ClaimedJob>> {
        let deadline = Instant::now() + Duration::from_millis(wait_ms as u64);
        loop {
            let seen = self.notifier.generation();
            if let Some(job) = self.try_claim(channel_id, lease_ms)? {
                return Ok(Some(job));
            }
            if self.is_shutting_down() {
                return Err(shutting_down());
            }
            let Some(left) = deadline
                .checked_duration_since(Instant::now())
                .filter(|d| !d.is_zero())
            else {
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
            let Some(left) = deadline
                .checked_duration_since(Instant::now())
                .filter(|d| !d.is_zero())
            else {
                return Ok(false);
            };
            self.notifier.wait_change(seen, left);
        }
    }

    /// Le shard propriétaire est encodé dans les 16 bits de poids fort du `reservation_id`.
    pub(crate) fn reservation_shard(&self, r: &Reservation) -> Result<usize> {
        let shard = (r.reservation_id >> 48) as usize;
        if shard < self.shards.len() {
            Ok(shard)
        } else {
            Err(EngineError::new(
                ErrorCode::StaleReservation,
                "unknown reservation",
            ))
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

    /// ACK groupés : une commande par shard concerné, résultats individuels dans l'ordre d'entrée.
    /// Chaque groupe n'est libéré qu'une fois ses transitions durables (cf. `defer`).
    pub fn ack_many(&self, reservations: Vec<Reservation>) -> Vec<Option<EngineError>> {
        let (tx, rx) = std::sync::mpsc::channel();
        self.ack_many_async(reservations, move |results| {
            let _ = tx.send(results);
        });
        rx.recv().unwrap_or_default()
    }

    /// Variante sans attente : `done` reçoit les résultats (dans l'ordre d'entrée) une fois tous les groupes terminés.
    pub fn ack_many_async(
        &self,
        reservations: Vec<Reservation>,
        done: impl FnOnce(Vec<Option<EngineError>>) + Send + 'static,
    ) {
        let n = reservations.len();
        let mut results = vec![None; n];
        let mut groups: Vec<(Vec<usize>, Vec<Reservation>)> =
            (0..self.shards.len()).map(|_| Default::default()).collect();
        for (i, r) in reservations.into_iter().enumerate() {
            match self.reservation_shard(&r) {
                Ok(s) => {
                    groups[s].0.push(i);
                    groups[s].1.push(r);
                }
                Err(e) => results[i] = Some(e),
            }
        }
        let parts = groups.iter().filter(|g| !g.1.is_empty()).count();
        if parts == 0 {
            return done(results);
        }
        type Join = (Vec<Option<EngineError>>, usize, Option<AckDone>);
        let join: Arc<Mutex<Join>> = Arc::new(Mutex::new((results, parts, Some(Box::new(done)))));
        for (s, (idx, rs)) in groups.into_iter().enumerate() {
            if rs.is_empty() {
                continue;
            }
            let join = join.clone();
            self.call_async(
                s,
                |reply| Cmd::AckMany(rs, reply),
                move |res| {
                    let mut j = join.lock().unwrap();
                    match res {
                        Ok(per_job) => {
                            for (i, r) in idx.into_iter().zip(per_job) {
                                j.0[i] = r;
                            }
                        }
                        Err(e) => {
                            for i in idx {
                                j.0[i] = Some(e.clone());
                            }
                        }
                    }
                    j.1 -= 1;
                    if j.1 == 0 {
                        let results = std::mem::take(&mut j.0);
                        let done = j.2.take();
                        drop(j);
                        if let Some(done) = done {
                            done(results);
                        }
                    }
                },
            );
        }
    }

    pub fn nack_async(
        &self,
        r: Reservation,
        kind: NackKind,
        done: impl FnOnce(Result<()>) + Send + 'static,
    ) {
        match self.reservation_shard(&r) {
            Ok(s) => self.call_async(s, |reply| Cmd::Nack(r, kind, reply), done),
            Err(e) => done(Err(e)),
        }
    }

    pub fn extend_lease_async(
        &self,
        r: Reservation,
        lease_ms: u32,
        done: impl FnOnce(Result<u64>) + Send + 'static,
    ) {
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
type AckDone = Box<dyn FnOnce(Vec<Option<EngineError>>) + Send>;

#[derive(Clone, Copy)]
struct ClaimManyArgs {
    channel_id: u16,
    lease_ms: u32,
    max: usize,
    skip: usize,
    start: usize,
}

pub(crate) type Pending = Vec<Box<dyn FnOnce(Option<&EngineError>) + Send>>;

/// Nombre maximal de commandes traitées avant un flush : borne la latence du group commit.
const MAX_BATCH: usize = 256;
/// Plafond de l'attente active : une configuration aberrante ne doit pas saturer un cœur.
/// Expérimental : une connexion commence ses claims par le shard de son dernier push.
pub(crate) fn claim_affinity() -> bool {
    static V: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *V.get_or_init(|| std::env::var("SOLID_ENGINE_CLAIM_AFFINITY").map_or(true, |v| v != "0"))
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
    *V.get_or_init(|| {
        parse_spin_us(
            "SOLID_ENGINE_SPIN_SHARD_US",
            std::env::var("SOLID_ENGINE_SPIN_SHARD_US").ok().as_deref(),
            MAX_SPIN_US,
        )
    })
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
    (
        SPIN_ATTEMPTS.load(Relaxed),
        SPIN_HITS.load(Relaxed),
        SPIN_SKIPPED.load(Relaxed),
    )
}

/// Exposant maximal du recul (0 = spin fixe, 2 = jusqu'à 3 attentes sautées, 6 = 63).
fn spin_backoff_cap() -> u32 {
    static V: std::sync::OnceLock<u32> = std::sync::OnceLock::new();
    *V.get_or_init(|| match std::env::var("SOLID_ENGINE_SPIN_BACKOFF") {
        Ok(s) => s
            .trim()
            .parse::<u32>()
            .map(|v| v.min(6))
            .unwrap_or_else(|_| {
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
    /// État encodé par le shard après l'envoi de tous les enregistrements qui le précèdent : la file étant
    /// FIFO, le writer le rattache à la séquence exacte de l'état sans que le shard attende l'I/O.
    Snapshot(Vec<u8>),
    /// camp35-E : fin de publication d'un snapshot par le worker (résultat, état rendu au writer).
    SnapDone(std::io::Result<()>, Vec<u8>),
}

/// camp35-E EXPERIMENTAL (SOLID_ENGINE_SNAPSHOT_WORKER=1, désactivé par défaut) : la publication du snapshot
/// (fichier tmp, sync_all, rename, sync du répertoire) est exécutée par un worker par shard ; le writer garde la
/// frontière WAL, la rotation et le nettoyage des segments, et continue à écrire les Append entre-temps.
fn snapshot_worker_enabled() -> bool {
    static V: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *V.get_or_init(|| std::env::var("SOLID_ENGINE_SNAPSHOT_WORKER").is_ok_and(|v| v == "1"))
}

struct SnapWork {
    job: solid_engine_storage::SnapshotJob,
    state: Vec<u8>,
    done: std::sync::mpsc::SyncSender<WalMsg>,
}

/// Libère les compteurs d'un snapshot terminé, abandonné ou en échec (mêmes effets que le chemin synchrone).
fn snapshot_release(shard_id: usize, state: Vec<u8>, flag: Option<&std::sync::atomic::AtomicBool>) {
    SNAP_LIVE_CAP.fetch_sub(state.capacity(), std::sync::atomic::Ordering::Relaxed);
    drop(state);
    SNAPSHOTS_IN_FLIGHT.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    SNAP_DONE_BY_SHARD[shard_id.min(15)].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    snapshot_gate_release();
    if let Some(f) = flag {
        f.store(false, std::sync::atomic::Ordering::Release);
    }
}

/// Tests : snapshots envoyés au writer et non terminés (tous shards), et maximum observé.
#[doc(hidden)]
pub static SNAPSHOTS_IN_FLIGHT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
#[doc(hidden)]
/// Diagnostic mémoire (camp33) : octets encodés / capacités des buffers de snapshot, en vol et au maximum.
pub static SNAP_LEN_MAX: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
pub static SNAP_CAP_MAX: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
pub static SNAP_LIVE_CAP: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
pub static SNAP_LIVE_CAP_MAX: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
pub static SNAP_COPY_LIVE: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
pub static SNAP_TOTAL_LIVE_MAX: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
pub static SNAP_COUNT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
pub static SNAPSHOTS_IN_FLIGHT_MAX: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Limite globale expérimentale (`SOLID_ENGINE_SNAPSHOT_MAX_CONCURRENT`, 0 = illimité, défaut) de snapshots
/// simultanés (encodage, file du writer et écriture) pour les shards qui passent par le writer.
/// Un shard qui n'obtient pas de place reporte son snapshot au prochain lot, sans jamais attendre.
fn snapshot_gate_limit() -> usize {
    static L: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *L.get_or_init(|| std::env::var("SOLID_ENGINE_SNAPSHOT_MAX_CONCURRENT").ok().and_then(|v| v.parse().ok()).unwrap_or(0))
}
pub static SNAP_GATE_ACTIVE: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
pub static SNAP_GATE_DEFERRED: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
pub static SNAP_BACKLOG_MAX: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static SNAP_ENCODING_MAX: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
pub static SNAP_ENCODING: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
pub static SNAP_DEFERRED_BY_SHARD: [std::sync::atomic::AtomicUsize; 16] = [const { std::sync::atomic::AtomicUsize::new(0) }; 16];
pub static SNAP_BACKLOG_NOW: [std::sync::atomic::AtomicU64; 16] = [const { std::sync::atomic::AtomicU64::new(0) }; 16];
pub static SNAP_DONE_BY_SHARD: [std::sync::atomic::AtomicUsize; 16] = [const { std::sync::atomic::AtomicUsize::new(0) }; 16];

fn snapshot_gate_acquire() -> bool {
    use std::sync::atomic::Ordering::SeqCst;
    let limit = snapshot_gate_limit();
    SNAP_GATE_ACTIVE.fetch_update(SeqCst, SeqCst, |n| (n < limit).then_some(n + 1)).is_ok()
}
fn snapshot_gate_release() {
    if snapshot_gate_limit() > 0 {
        SNAP_GATE_ACTIVE.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

/// Modes `balanced` et `strict` : un thread unique possède le WAL (encodage, écriture, fsync).
/// En `balanced` le shard répond sans attendre le disque ; en `strict` les réponses sont
/// transmises au writer, qui les libère après un fsync commun à toutes les soumissions en attente.
/// Le shard n'attend jamais le disque, et l'écriture et le fsync concurrents ne le ralentissent plus.
/// Une erreur d'I/O est publiée dans `error` et fait passer le shard en échec.
// EXPERIMENTAL (camp28) : retard du writer par shard, lu à 10 Hz si SOLID_ENGINE_WAL_LAG est défini.
pub static LAG_SENT: [std::sync::atomic::AtomicU64; 16] = [const { std::sync::atomic::AtomicU64::new(0) }; 16];
pub static LAG_WRITTEN: [std::sync::atomic::AtomicU64; 16] = [const { std::sync::atomic::AtomicU64::new(0) }; 16];
pub static LAG_SYNCED: [std::sync::atomic::AtomicU64; 16] = [const { std::sync::atomic::AtomicU64::new(0) }; 16];
pub static BYTES_SENT: [std::sync::atomic::AtomicU64; 16] = [const { std::sync::atomic::AtomicU64::new(0) }; 16];
pub static BYTES_WRITTEN: [std::sync::atomic::AtomicU64; 16] = [const { std::sync::atomic::AtomicU64::new(0) }; 16];
/// Maximum observé de (octets envoyés non écrits + fournée en cours d'envoi) et taille maximale d'une fournée.
pub static BYTES_PEND_MAX: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static BYTES_BATCH_MAX: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static BP_WAIT_US: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// camp35-E1 : compteurs de snapshots [demandés, fusionnés, démarrés, réussis, échoués], tous shards.
pub static SNAP_CNT: [std::sync::atomic::AtomicU64; 5] = [const { std::sync::atomic::AtomicU64::new(0) }; 5];
fn snap_cnt(i: usize) {
    SNAP_CNT[i].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}
pub static LAG_SNAP: [std::sync::atomic::AtomicU64; 16] = [const { std::sync::atomic::AtomicU64::new(0) }; 16];

/// Diagnostic du writer (camp35), cumulé par shard et lu à 10 Hz si SOLID_ENGINE_WAL_LAG est défini :
/// [appends, records, batch_max, sync_n, sync_ns, sync_max_ns, idle_ns, wakes, append_ns, sync_timer_n].
const DIAG_FIELDS: usize = 10;
pub static DIAG: [[std::sync::atomic::AtomicU64; DIAG_FIELDS]; 16] = [const { [const { std::sync::atomic::AtomicU64::new(0) }; DIAG_FIELDS] }; 16];

/// camp35-D : journal d'événements du writer (shard, genre 0=append 1=sync 2=snapshot, début µs, durée µs).
/// Les appends < 100 µs ne sont pas journalisés (ils sont comptés dans l'histogramme `DIAG_H`).
static DIAG_EV: std::sync::Mutex<Vec<(u8, u8, u64, u64)>> = std::sync::Mutex::new(Vec::new());
static DIAG_H: [[std::sync::atomic::AtomicU64; 24]; 4] = [const { [const { std::sync::atomic::AtomicU64::new(0) }; 24] }; 4];
fn diag_epoch() -> Instant {
    solid_engine_storage::diag_epoch()
}
fn diag_event(shard: usize, kind: usize, start: Instant) {
    let dur = start.elapsed().as_micros() as u64;
    let b = (64 - dur.max(1).leading_zeros() as usize - 1).min(23);
    DIAG_H[kind][b].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    if kind != 0 || dur >= 100 {
        let st = start.duration_since(diag_epoch()).as_micros() as u64;
        DIAG_EV.lock().unwrap().push((shard as u8, kind as u8, st, dur));
    }
}

fn diag_on() -> bool {
    solid_engine_storage::DIAG_ON.load(std::sync::atomic::Ordering::Relaxed)
}

fn diag_add(shard: usize, field: usize, v: u64) {
    DIAG[shard][field].fetch_add(v, std::sync::atomic::Ordering::Relaxed);
}

fn start_lag_printer(shards: usize) {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        if std::env::var("SOLID_ENGINE_WAL_LAG").is_err() {
            return;
        }
        solid_engine_storage::DIAG_ON.store(true, std::sync::atomic::Ordering::Relaxed);
        diag_epoch();
        // Retard échantillonné à 1 kHz : "wal_pend t_us shard:unwritten,unsynced ..." (écrit 10 fois par seconde).
        std::thread::spawn(move || {
            let mut buf = String::new();
            let mut last = Instant::now();
            loop {
                std::thread::sleep(Duration::from_millis(1));
                let l = std::sync::atomic::Ordering::Relaxed;
                buf += &format!("wal_pend {}", diag_epoch().elapsed().as_micros());
                for i in 0..shards.min(2) {
                    let (sent, w, sy) = (LAG_SENT[i].load(l), LAG_WRITTEN[i].load(l), LAG_SYNCED[i].load(l));
                    buf += &format!(" {},{}", sent.saturating_sub(w), w.saturating_sub(sy));
                }
                buf.push('\n');
                if last.elapsed() >= Duration::from_millis(100) {
                    last = Instant::now();
                    let ev: Vec<_> = std::mem::take(&mut *DIAG_EV.lock().unwrap());
                    for (sh, k, st, d) in ev {
                        buf += &format!("wal_ev {sh} {k} {st} {d}\n");
                    }
                    let sn: Vec<_> = std::mem::take(&mut *solid_engine_storage::DIAG_SNAP.lock().unwrap());
                    for (sh, st, p) in sn {
                        buf += &format!("wal_snap {sh} {st} {}\n", p.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(" "));
                    }
                    let h: Vec<String> = (0..4).map(|k| DIAG_H[k].iter().map(|x| x.load(l).to_string()).collect::<Vec<_>>().join(",")).collect();
                    buf += &format!("wal_hist {}\n", h.join(" "));
                    eprint!("{buf}");
                    buf.clear();
                }
            }
        });
        std::thread::spawn(move || {
            let t0 = Instant::now();
            loop {
                std::thread::sleep(Duration::from_millis(100));
                let l = std::sync::atomic::Ordering::Relaxed;
                let mut line = format!(
                    "wal_lag t={} bp={} pbmax={} bbmax={} bw={} snapc={}",
                    t0.elapsed().as_millis(),
                    BP_WAIT_US.load(l),
                    BYTES_PEND_MAX.load(l),
                    BYTES_BATCH_MAX.load(l),
                    (0..shards.min(16)).map(|i| BYTES_WRITTEN[i].load(l)).sum::<u64>(),
                    SNAP_CNT.iter().map(|c| c.load(l).to_string()).collect::<Vec<_>>().join(",")
                );
                for i in 0..shards.min(16) {
                    let l = std::sync::atomic::Ordering::Relaxed;
                    line += &format!(" {i}:{},{},{},{}", LAG_SENT[i].load(l), LAG_WRITTEN[i].load(l), LAG_SYNCED[i].load(l), LAG_SNAP[i].load(l));
                }
                eprintln!("{line}");
                let l = std::sync::atomic::Ordering::Relaxed;
                let mut d = format!("wal_diag t={}", t0.elapsed().as_millis());
                for i in 0..shards.min(16) {
                    let v: Vec<u64> = DIAG[i].iter().map(|x| x.load(l)).collect();
                    if v.iter().all(|x| *x == 0) {
                        continue;
                    }
                    d += &format!(
                        " {i}:{},{},{},{},{},{},{},{},{},{},{},{}",
                        v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7], v[8], v[9],
                        solid_engine_storage::DIAG_ENCODE_NS[i].load(l),
                        solid_engine_storage::DIAG_WRITE_NS[i].load(l)
                    );
                }
                eprintln!("{d}");
            }
        });
    });
}

struct WalWriter {
    shard_id: usize,
    tx: Option<std::sync::mpsc::SyncSender<WalMsg>>,
    error: Arc<std::sync::Mutex<Option<String>>>,
    handle: Option<std::thread::JoinHandle<()>>,
    /// Vrai de l'envoi d'un snapshot jusqu'à la fin de son traitement par le writer (succès ou échec).
    /// Un seul snapshot par shard est en vol : sans cela, des snapshots complets s'empilent
    /// dans la file quand le disque est plus lent que la production, et la mémoire n'est pas bornée.
    snapshot_in_flight: Arc<std::sync::atomic::AtomicBool>,
    /// Émetteur remis aux jobs de snapshot ; vidé à l'arrêt pour que la file puisse se déconnecter.
    done_slot: Arc<std::sync::Mutex<Option<std::sync::mpsc::SyncSender<WalMsg>>>>,
}

impl Drop for WalWriter {
    fn drop(&mut self) {
        self.done_slot.lock().unwrap().take();
    }
}

impl WalWriter {
    fn spawn(shard_id: usize, mut wal: Wal, sync_interval: Duration) -> Self {
        // File bornée : si le disque prend du retard, le shard est ralenti plutôt que la mémoire saturée.
        let (tx, rx) = std::sync::mpsc::sync_channel::<WalMsg>(65_536);
        let error = Arc::new(std::sync::Mutex::new(None::<String>));
        let slot = error.clone();
        let in_flight = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let writer_flag = in_flight.clone();
        let done_slot = Arc::new(std::sync::Mutex::new(snapshot_worker_enabled().then(|| tx.clone())));
        let done_slot_thread = done_slot.clone();
        {
            let l = std::sync::atomic::Ordering::Relaxed;
            LAG_SENT[shard_id].store(wal.last_seq(), l);
            LAG_WRITTEN[shard_id].store(wal.last_seq(), l);
            LAG_SYNCED[shard_id].store(wal.last_seq(), l);
            start_lag_printer(16);
        }
        let handle = std::thread::Builder::new()
            .name(format!("solid-engine-wal-{shard_id}"))
            .spawn(move || {
                let fail = |what: &str, e: std::io::Error| {
                    slot.lock().unwrap().get_or_insert(format!("{what}: {e}"));
                };
                let mut due: Option<Instant> = None;
                let mut carry: Option<WalMsg> = None;
                let worker = snapshot_worker_enabled();
                let mut work_tx: Option<std::sync::mpsc::SyncSender<SnapWork>> = None;
                // Snapshot en cours de publication, et le plus récent en attente (les plus anciens sont abandonnés).
                let mut inflight = false;
                let mut snap_seq_inflight = 0u64;
                let mut pending_snap: Option<(Vec<u8>, u64)> = None;
                macro_rules! start_snapshot {
                    ($state:expr, $upto:expr) => {{
                        let state: Vec<u8> = $state;
                        let upto: u64 = $upto;
                        let td = diag_on().then(Instant::now);
                        match wal.snapshot_begin(upto) {
                            Ok(None) => snapshot_release(shard_id, state, Some(&writer_flag)),
                            Ok(Some(job)) => {
                                snap_cnt(2);
                                due = None;
                                let done = done_slot_thread.lock().unwrap().clone();
                                match done {
                                    Some(done) => {
                                        let wtx = work_tx.get_or_insert_with(|| {
                                            let (wtx, wrx) = std::sync::mpsc::sync_channel::<SnapWork>(1);
                                            let _ = std::thread::Builder::new()
                                                .name(format!("solid-engine-snap-{shard_id}"))
                                                .spawn(move || {
                                                    for w in wrx {
                                                        let t = Instant::now();
                                                        let r = w.job.publish(&w.state);
                                                        if diag_on() {
                                                            diag_event(shard_id, 3, t);
                                                        }
                                                        let _ = w.done.send(WalMsg::SnapDone(r, w.state));
                                                    }
                                                });
                                            wtx
                                        });
                                        snap_seq_inflight = job.seq;
                                        match wtx.send(SnapWork { job, state, done }) {
                                            Ok(()) => inflight = true,
                                            Err(e) => {
                                                fail("WAL snapshot failed", std::io::Error::other("snapshot worker stopped"));
                                                snapshot_release(shard_id, e.0.state, Some(&writer_flag));
                                            }
                                        }
                                    }
                                    None => snapshot_release(shard_id, state, Some(&writer_flag)),
                                }
                            }
                            Err(e) => {
                                fail("WAL snapshot failed", e);
                                snapshot_release(shard_id, state, Some(&writer_flag));
                            }
                        }
                        if let Some(td) = td {
                            diag_event(shard_id, 2, td);
                        }
                    }};
                }
                let mut tally =
                    crate::stats::Tally::default().with_cpu_role(crate::stats::C::CpuWalNs);
                loop {
                    let carry_none = carry.is_none();
                    let idle_from = (carry_none && diag_on()).then(Instant::now);
                    let msg = match carry.take() {
                        Some(m) => Ok(m),
                        None => match due {
                            Some(t) => rx.recv_timeout(t.saturating_duration_since(Instant::now())),
                            None => rx.recv().map_err(|_| RecvTimeoutError::Disconnected),
                        },
                    };
                    if let Some(t) = idle_from {
                        let ns = t.elapsed().as_nanos() as u64;
                        diag_add(shard_id, 6, ns);
                        if ns > 50_000 {
                            diag_add(shard_id, 7, 1);
                        }
                    }
                    if let (true, Ok(m)) = (carry_none && wal_trace::on(), &msg) {
                        wal_trace::received(match m {
                            WalMsg::Append(r) | WalMsg::Commit(r, _) => r.len(),
                            _ => 0,
                        });
                    }
                    if msg.is_ok() {
                        tally.add(crate::stats::C::WalMsgs, 1);
                        tally.tick();
                    }
                    match msg {
                        Ok(WalMsg::Append(records)) => {
                            if slot.lock().unwrap().is_none() {
                                let t0 = wal_trace::on().then(Instant::now);
                                let td = diag_on().then(Instant::now);
                                let r = wal.append(&records);
                                if let Some(td) = td {
                                    diag_add(shard_id, 0, 1);
                                    diag_add(shard_id, 1, records.len() as u64);
                                    DIAG[shard_id][2].fetch_max(records.len() as u64, std::sync::atomic::Ordering::Relaxed);
                                    diag_add(shard_id, 8, td.elapsed().as_nanos() as u64);
                                    diag_event(shard_id, 0, td);
                                }
                                if let Some(t0) = t0 {
                                    wal_trace::appended(t0.elapsed());
                                }
                                match r {
                                    Ok(()) => {
                                        BYTES_WRITTEN[shard_id].fetch_add(
                                            records.iter().map(|r| r.wire_len()).sum::<u64>(),
                                            std::sync::atomic::Ordering::Relaxed,
                                        );
                                        LAG_WRITTEN[shard_id].store(wal.last_seq(), std::sync::atomic::Ordering::Relaxed);
                                        due = due.or(Some(Instant::now() + sync_interval))
                                    }
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
                                    } else {
                                        LAG_WRITTEN[shard_id].store(wal.last_seq(), std::sync::atomic::Ordering::Relaxed);
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
                                let t0 = wal_trace::on().then(Instant::now);
                                let td = diag_on().then(Instant::now);
                                if let Err(e) = wal.sync() {
                                    fail("WAL sync failed", e);
                                } else {
                                    LAG_SYNCED[shard_id].store(wal.last_seq(), std::sync::atomic::Ordering::Relaxed);
                                }
                                if let Some(td) = td {
                                    let ns = td.elapsed().as_nanos() as u64;
                                    diag_add(shard_id, 3, 1);
                                    diag_add(shard_id, 4, ns);
                                    DIAG[shard_id][5].fetch_max(ns, std::sync::atomic::Ordering::Relaxed);
                                    diag_event(shard_id, 1, td);
                                }
                                if let Some(t0) = t0 {
                                    wal_trace::synced(t0.elapsed(), batch.len());
                                }
                            }
                            let failure = slot
                                .lock()
                                .unwrap()
                                .clone()
                                .map(|e| io_failure("WAL failure", e));
                            for p in batch.into_iter().flatten() {
                                p(failure.as_ref());
                            }
                        }
                        Ok(WalMsg::Snapshot(state)) if worker => {
                            snap_cnt(0);
                            let upto = wal.last_seq();
                            if slot.lock().unwrap().is_some() {
                                snapshot_release(shard_id, state, Some(&writer_flag));
                            } else if inflight {
                                if let Some((old, _)) = pending_snap.replace((state, upto)) {
                                    snap_cnt(1);
                                    snapshot_release(shard_id, old, None);
                                }
                            } else {
                                start_snapshot!(state, upto);
                            }
                        }
                        Ok(WalMsg::SnapDone(result, state)) => {
                            inflight = false;
                            let td = diag_on().then(Instant::now);
                            match result {
                                Ok(()) if slot.lock().unwrap().is_none() => match wal.snapshot_finish(snap_seq_inflight) {
                                    Ok(()) => {
                                        snap_cnt(3);
                                        LAG_SNAP[shard_id].store(snap_seq_inflight, std::sync::atomic::Ordering::Relaxed)
                                    }
                                    Err(e) => {
                                        snap_cnt(4);
                                        fail("WAL snapshot failed", e)
                                    }
                                },
                                Ok(()) => {}
                                Err(e) => {
                                    snap_cnt(4);
                                    fail("WAL snapshot failed", e)
                                }
                            }
                            if let Some(td) = td {
                                diag_event(shard_id, 2, td);
                            }
                            snapshot_release(shard_id, state, Some(&writer_flag));
                            if let Some((next, next_upto)) = pending_snap.take() {
                                if slot.lock().unwrap().is_none() {
                                    start_snapshot!(next, next_upto);
                                } else {
                                    snapshot_release(shard_id, next, Some(&writer_flag));
                                }
                            }
                        }
                        Ok(WalMsg::Snapshot(state)) => {
                            snap_cnt(0);
                            if slot.lock().unwrap().is_none() {
                                snap_cnt(2);
                                let t0 = wal_trace::on().then(Instant::now);
                                let td = diag_on().then(Instant::now);
                                // `Wal::snapshot` n'effectue plus de copie complète de l'état.
                                let copy = 0usize;
                                {
                                    use std::sync::atomic::Ordering::Relaxed;
                                    let c = SNAP_COPY_LIVE.fetch_add(copy, Relaxed) + copy;
                                    SNAP_TOTAL_LIVE_MAX.fetch_max(c + SNAP_LIVE_CAP.load(Relaxed), Relaxed);
                                }
                                let snap_result = wal.snapshot(&state);
                                SNAP_COPY_LIVE.fetch_sub(copy, std::sync::atomic::Ordering::Relaxed);
                                match snap_result {
                                    Ok(()) => {
                                        snap_cnt(3);
                                        due = None;
                                        LAG_SNAP[shard_id].store(wal.last_seq(), std::sync::atomic::Ordering::Relaxed);
                                    }
                                    Err(e) => {
                                        snap_cnt(4);
                                        fail("WAL snapshot failed", e)
                                    }
                                }
                                if let Some(td) = td {
                                    diag_event(shard_id, 2, td);
                                }
                                if let Some(t0) = t0 {
                                    wal_trace::snapshot_io(t0.elapsed());
                                }
                            }
                            // Libéré sur tous les chemins ; une erreur d'écriture reste publiée par `fail`.
                            SNAP_LIVE_CAP.fetch_sub(state.capacity(), std::sync::atomic::Ordering::Relaxed);
                            drop(state);
                            SNAPSHOTS_IN_FLIGHT.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                            SNAP_DONE_BY_SHARD[shard_id.min(15)].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            snapshot_gate_release();
                            writer_flag.store(false, std::sync::atomic::Ordering::Release);
                        }
                        Err(RecvTimeoutError::Timeout) => {
                            due = None;
                            let t0 = wal_trace::on().then(Instant::now);
                            let td = diag_on().then(Instant::now);
                            if let Err(e) = wal.sync() {
                                fail("WAL sync failed", e);
                            } else {
                                LAG_SYNCED[shard_id].store(wal.last_seq(), std::sync::atomic::Ordering::Relaxed);
                            }
                            if let Some(td) = td {
                                let ns = td.elapsed().as_nanos() as u64;
                                diag_add(shard_id, 3, 1);
                                diag_add(shard_id, 4, ns);
                                DIAG[shard_id][5].fetch_max(ns, std::sync::atomic::Ordering::Relaxed);
                                diag_add(shard_id, 9, 1);
                                diag_event(shard_id, 1, td);
                            }
                            if let Some(t0) = t0 {
                                wal_trace::synced(t0.elapsed(), 0);
                            }
                        }
                        Err(RecvTimeoutError::Disconnected) => {
                            if let Some((p, _)) = pending_snap.take() {
                                snapshot_release(shard_id, p, Some(&writer_flag));
                            }
                            if let Err(e) = wal.sync() {
                                fail("WAL sync failed", e);
                            }
                            return;
                        }
                    }
                }
            })
            .expect("spawn WAL writer");
        Self {
            tx: Some(tx),
            error,
            handle: Some(handle),
            snapshot_in_flight: in_flight,
            shard_id,
            done_slot,
        }
    }

    /// EXPERIMENTAL (camp29) : borne le nombre de records envoyés mais pas encore écrits par shard
    /// (SOLID_ENGINE_WAL_MAX_PENDING_RECORDS, 0 = illimité). Le writer ne dépend jamais du shard : pas d'interblocage.
    /// Le seuil peut être dépassé d'une fournée au plus.
    /// Attend que le retard du writer repasse sous les plafonds (records et/ou octets). Plafond souple :
    /// la fournée qui suit est envoyée dès que le retard est sous le plafond, donc le retard peut dépasser
    /// le plafond d'au plus une fournée. Une fournée plus grosse que le plafond passe quand le retard est nul.
    fn wait_for_capacity(&self) {
        static MAX: std::sync::OnceLock<(u64, u64)> = std::sync::OnceLock::new();
        let (max_rec, max_bytes) = *MAX.get_or_init(|| {
            let get = |k: &str| std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(0);
            (get("SOLID_ENGINE_WAL_MAX_PENDING_RECORDS"), get("SOLID_ENGINE_WAL_MAX_PENDING_BYTES"))
        });
        if max_rec == 0 && max_bytes == 0 {
            return;
        }
        let l = std::sync::atomic::Ordering::Relaxed;
        let over = || {
            (max_rec > 0 && LAG_SENT[self.shard_id].load(l).saturating_sub(LAG_WRITTEN[self.shard_id].load(l)) > max_rec)
                || (max_bytes > 0
                    && BYTES_SENT[self.shard_id].load(l).saturating_sub(BYTES_WRITTEN[self.shard_id].load(l)) > max_bytes)
        };
        if !over() {
            return;
        }
        let t0 = Instant::now();
        while over() {
            // Un writer en échec n'écrit plus : on laisse passer pour que l'erreur soit publiée.
            if self.take_error().is_some() || self.handle.as_ref().is_none_or(|h| h.is_finished()) {
                break;
            }
            std::thread::sleep(Duration::from_micros(20));
        }
        BP_WAIT_US.fetch_add(t0.elapsed().as_micros() as u64, l);
    }

    fn take_error(&self) -> Option<String> {
        self.error.lock().unwrap().clone()
    }

    /// Arrêt propre : ferme la file, le thread synchronise puis se termine.
    fn finish(mut self) {
        self.tx = None;
        self.done_slot.lock().unwrap().take();
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
    single_flight: bool,
}

fn open_shard(
    id: usize,
    cfg: &Arc<Config>,
) -> std::result::Result<(Shard, Option<Durable>), String> {
    let storage = &cfg.storage;
    if storage.durability == Durability::Memory {
        return Ok((Shard::new(id as u16, cfg.clone()), None));
    }
    let dir = Path::new(storage.data_dir.as_deref().unwrap()).join(format!("shard-{id}"));
    let started = Instant::now();
    let (shard, wal) =
        recover_shard(&dir, id as u16, cfg.clone()).map_err(|e| format!("shard {id}: {e}"))?;
    eprintln!(
        "shard {id} recovered in {:?} (last sequence {}, replayed {})",
        started.elapsed(),
        wal.last_seq(),
        wal.records_since_snapshot()
    );
    let since_snapshot = wal.records_since_snapshot();
    let mode = storage.durability;
    let (wal, writer) = if mode != Durability::Memory {
        (
            None,
            Some(WalWriter::spawn(
                id,
                wal,
                Duration::from_millis(storage.sync_interval_ms),
            )),
        )
    } else {
        (Some(wal), None)
    };
    Ok((
        shard,
        Some(Durable {
            wal,
            writer,
            mode,
            since_snapshot,
            single_flight: std::env::var("SOLID_ENGINE_SNAPSHOT_SINGLE_FLIGHT")
                .map_or(storage.snapshot_single_flight, |v| v == "1"),
        }),
    ))
}

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
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
    let t_log = shard_phase::on().then(Instant::now);
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
                        let t0 = wal_trace::on().then(Instant::now);
                        if t0.is_some() {
                            wal_trace::before_send();
                        }
                        LAG_SENT[w.shard_id].fetch_add(count, std::sync::atomic::Ordering::Relaxed);
                        let sent =
                            w.tx.as_ref()
                                .is_some_and(|tx| tx.send(WalMsg::Commit(records, batch)).is_ok());
                        if let Some(t0) = t0 {
                            wal_trace::sent(t0.elapsed());
                        }
                        if !sent {
                            *failed = Some(io_failure("WAL writer", "stopped"));
                        }
                    }
                } else if count > 0 {
                    let t0 = wal_trace::on().then(Instant::now);
                    if t0.is_some() {
                        wal_trace::before_send();
                    }
                    w.wait_for_capacity();
                    let l = std::sync::atomic::Ordering::Relaxed;
                    let bytes: u64 = records.iter().map(|r| r.wire_len()).sum();
                    let sent_b = BYTES_SENT[w.shard_id].fetch_add(bytes, l) + bytes;
                    BYTES_PEND_MAX.fetch_max(sent_b.saturating_sub(BYTES_WRITTEN[w.shard_id].load(l)), l);
                    BYTES_BATCH_MAX.fetch_max(bytes, l);
                    LAG_SENT[w.shard_id].fetch_add(count, l);
                    let sent =
                        w.tx.as_ref()
                            .is_some_and(|tx| tx.send(WalMsg::Append(records)).is_ok());
                    if let Some(t0) = t0 {
                        wal_trace::sent(t0.elapsed());
                    }
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
        let every = std::env::var("SOLID_ENGINE_SNAPSHOT_EVERY")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(cfg.storage.snapshot_every_records);
        // `since_snapshot` continue de croître tant qu'un snapshot est en vol : un seul nouveau snapshot
        // suit sa fin, puis le compteur repart de zéro.
        let busy = d.single_flight
            && d.writer.as_ref().is_some_and(|w| w.snapshot_in_flight.load(std::sync::atomic::Ordering::Acquire));
        let due_now = failed.is_none() && every > 0 && d.since_snapshot >= every && !busy;
        SNAP_BACKLOG_NOW[(shard.id() as usize).min(15)].store(d.since_snapshot, std::sync::atomic::Ordering::Relaxed);
        if due_now {
            SNAP_BACKLOG_MAX.fetch_max(d.since_snapshot, std::sync::atomic::Ordering::Relaxed);
        }
        let gated = due_now && d.writer.is_some() && snapshot_gate_limit() > 0;
        let admitted = !gated || snapshot_gate_acquire();
        if gated && !admitted {
            SNAP_GATE_DEFERRED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            SNAP_DEFERRED_BY_SHARD[(shard.id() as usize).min(15)].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        if due_now && admitted {
            let t_snap = Instant::now();
            let enc = SNAP_ENCODING.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
            SNAP_ENCODING_MAX.fetch_max(enc, std::sync::atomic::Ordering::Relaxed);
            let state = shard.encode_snapshot();
            SNAP_ENCODING.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            {
                use std::sync::atomic::Ordering::Relaxed;
                SNAP_LEN_MAX.fetch_max(state.len(), Relaxed);
                SNAP_CAP_MAX.fetch_max(state.capacity(), Relaxed);
                SNAP_COUNT.fetch_add(1, Relaxed);
                let live = SNAP_LIVE_CAP.fetch_add(state.capacity(), Relaxed) + state.capacity();
                SNAP_LIVE_CAP_MAX.fetch_max(live, Relaxed);
            }
            wal_trace::snapshot_encode(t_snap.elapsed());
            let result = if let Some(w) = &d.writer {
                // Le shard n'attend pas l'I/O : une erreur du writer remonte par `take_error`.
                // Diagnostic : encode puis abandonne, pour isoler le coût de l'I/O du snapshot.
                let sent = if std::env::var_os("SOLID_ENGINE_SNAPSHOT_SKIP_IO").is_some() {
                    SNAP_LIVE_CAP.fetch_sub(state.capacity(), std::sync::atomic::Ordering::Relaxed);
                    drop(state);
                    if gated {
                        snapshot_gate_release();
                    }
                    Some(Ok(()))
                } else {
                    w.snapshot_in_flight.store(true, std::sync::atomic::Ordering::Release);
                    let n = SNAPSHOTS_IN_FLIGHT.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                    SNAPSHOTS_IN_FLIGHT_MAX.fetch_max(n, std::sync::atomic::Ordering::SeqCst);
                    let sent = w.tx.as_ref().map(|tx| tx.send(WalMsg::Snapshot(state)));
                    if !matches!(sent, Some(Ok(()))) {
                        SNAPSHOTS_IN_FLIGHT.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                        if gated {
                            snapshot_gate_release();
                        }
                        w.snapshot_in_flight.store(false, std::sync::atomic::Ordering::Release);
                    }
                    sent
                };
                match sent {
                    Some(Ok(())) => Ok(()),
                    _ => Err(std::io::Error::other("WAL writer stopped")),
                }
            } else {
                d.wal.as_mut().unwrap().snapshot(&state)
            };
            wal_trace::snapshot(t_snap.elapsed());
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
    let t_reply = shard_phase::on().then(Instant::now);
    if let Some(t) = t_log {
        shard_phase::add(shard_phase::LOG, t_reply.unwrap() - t);
    }
    for p in pending.drain(..) {
        p(failed.as_ref());
    }
    if let Some(t) = t_reply {
        shard_phase::add(shard_phase::REPLY, t.elapsed());
    }
}

/// Applique une commande au shard ; renvoie vrai pour `Shutdown`.
fn apply_cmd(
    shard: &mut Shard,
    pending: &mut Pending,
    failed: &Option<EngineError>,
    cmd: Cmd,
    now: u64,
) -> bool {
    match cmd {
        Cmd::Push(r, tx) => defer(pending, tx, shard.push(r, now)),
        Cmd::PushMany(rs, tx) => {
            let results = rs.into_iter().map(|r| shard.push(r, now)).collect();
            defer(pending, tx, Ok(results))
        }
        Cmd::Claim {
            channel_id,
            lease_ms,
            reply,
        } => defer(pending, reply, shard.claim(channel_id, lease_ms, now)),
        Cmd::ClaimMany {
            channel_id,
            lease_ms,
            max,
            reply,
        } => {
            let mut jobs = Vec::with_capacity(max.min(64));
            let mut error = None;
            while jobs.len() < max {
                match shard.claim(channel_id, lease_ms, now) {
                    Ok(Some(job)) => jobs.push(job),
                    Ok(None) => break,
                    Err(e) => {
                        error = Some(e);
                        break;
                    }
                }
            }
            // Les jobs déjà réservés sont rendus même si la suite échoue.
            let result = match error {
                Some(e) if jobs.is_empty() => Err(e),
                _ => Ok(jobs),
            };
            defer(pending, reply, result)
        }
        Cmd::ClaimDispatch {
            channel_id,
            lease_ms,
            max,
            sent,
            reply,
        } => {
            let t_start = Instant::now();
            let mut bodies = Vec::new();
            let mut body = solid_engine_protocol::DispatchBody::new();
            let mut got = 0;
            let mut error = None;
            while got < max {
                match shard.claim_with(channel_id, lease_ms, now, |m, p| body.push(m, p)) {
                    Ok(Some(())) => got += 1,
                    Ok(None) => break,
                    Err(e) => {
                        error = Some(e);
                        break;
                    }
                }
                if body.len() >= crate::net::DISPATCH_MAX_BYTES {
                    bodies.push(std::mem::take(&mut body));
                }
            }
            if !body.is_empty() {
                bodies.push(body);
            }
            if claim_trace::on() {
                claim_trace::record(sent, t_start, Instant::now(), got);
            }
            let result = match error {
                Some(e) if bodies.is_empty() => Err(e),
                _ => Ok(bodies),
            };
            defer(pending, reply, result)
        }
        Cmd::AckMany(rs, tx) => {
            let results = rs.into_iter().map(|r| shard.ack(r, now).err()).collect();
            defer(pending, tx, Ok(results))
        }
        Cmd::Ack(r, tx) => defer(pending, tx, shard.ack(r, now)),
        Cmd::Nack(r, k, tx) => defer(pending, tx, shard.nack(r, k, now)),
        Cmd::Extend(r, ms, tx) => defer(pending, tx, shard.extend_lease(r, ms, now)),
        Cmd::Cancel(id, tx) => defer(pending, tx, shard.cancel(id, now)),
        Cmd::Get(id, tx) => reply_read(tx, failed, || shard.get(id, now)),
        Cmd::Stats(ch, tx) => reply_read(tx, failed, || Ok(shard.stats(ch, now))),
        Cmd::Shutdown => return true,
    }
    false
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
        if shard.take_shrink_finished() {
            crate::stats::trim_memory();
        }
        if mem_stats && last_stats.elapsed() >= crate::stats::mem_stats_interval() {
            last_stats = Instant::now();
            let [jobs, cap, ret, leases, sched, ready] = shard.sizes();
            eprintln!("mem_stats shard={id} jobs={jobs} cap={cap} retention={ret} leases={leases} scheduled={sched} ready={ready}");
            eprintln!("mem_report shard={id} {:?}", shard.mem_report());
            if id == 0 {
                eprintln!("mem_process {}", crate::stats::process_memory());
            }
        }
        flush(&mut shard, &mut durable, &mut failed, &mut pending, &cfg);
        if shard.take_ready_flag() {
            notifier.notify();
        }

        let wait = shard
            .next_deadline()
            .map(|t| Duration::from_millis(t.saturating_sub(now)));
        let wait = if mem_stats {
            Some(wait.map_or(Duration::from_secs(5), |w| w.min(Duration::from_secs(5))))
        } else {
            wait
        };
        // Courte attente active avant de dormir : évite un réveil (appel système) par commande.
        let t_wait = shard_phase::on().then(Instant::now);
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

        let t_apply = shard_phase::on().then(Instant::now);
        if let (Some(w), Some(a)) = (t_wait, t_apply) {
            shard_phase::add(shard_phase::WAIT, a - w);
        }
        let mut stop = false;
        let mut next = first;
        let mut handled = 0;
        while let Some(cmd) = next {
            if apply_cmd(&mut shard, &mut pending, &failed, cmd, now_ms()) {
                stop = true;
                break;
            }
            handled += 1;
            next = if handled < MAX_BATCH {
                rx.try_recv().ok()
            } else {
                None
            };
        }
        if handled > 0 {
            tally.add(crate::stats::C::ShardCommands, handled as u64);
            tally.add(crate::stats::C::ShardBatches, 1);
            tally.tick();
        }
        let t_flush = shard_phase::on().then(Instant::now);
        if let (Some(a), Some(f)) = (t_apply, t_flush) {
            shard_phase::add(shard_phase::APPLY, f - a);
            shard_phase::count(handled as u64);
        }
        flush(&mut shard, &mut durable, &mut failed, &mut pending, &cfg);
        if let Some(f) = t_flush {
            shard_phase::add(shard_phase::FLUSH, f.elapsed());
        }
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

/// Latences des `ClaimDispatch` (SOLID_ENGINE_DISPATCH_TRACE) : attente en file, exécution, total.
pub mod claim_trace {
    use super::*;
    const BUCKETS: usize = 200;
    pub struct Hist([AtomicU64; BUCKETS]);
    impl Hist {
        pub const fn new() -> Self {
            Self([const { AtomicU64::new(0) }; BUCKETS])
        }
        pub fn record(&self, d: Duration) {
            let ns = (d.as_nanos() as f64).max(1.0);
            let i = ((ns.log2() * 4.0) as usize).min(BUCKETS - 1);
            self.0[i].fetch_add(1, Ordering::Relaxed);
        }
        pub fn report(&self) -> String {
            let counts: Vec<u64> = self.0.iter().map(|c| c.load(Ordering::Relaxed)).collect();
            let total: u64 = counts.iter().sum();
            let sum: f64 = counts.iter().enumerate().map(|(i, c)| *c as f64 * 2f64.powf((i as f64 + 0.5) / 4.0)).sum();
            let pct = |q: f64| {
                let want = (total as f64 * q).ceil() as u64;
                let mut acc = 0;
                for (i, c) in counts.iter().enumerate() {
                    acc += c;
                    if acc >= want {
                        return 2f64.powf((i as f64 + 1.0) / 4.0) / 1000.0;
                    }
                }
                0.0
            };
            format!(
                "mean={:.1} p50={:.1} p95={:.1} p99={:.1}",
                sum / total.max(1) as f64 / 1000.0,
                pct(0.5),
                pct(0.95),
                pct(0.99)
            )
        }
    }
    static QUEUE: Hist = Hist::new();
    static EXEC: Hist = Hist::new();
    static TOTAL: Hist = Hist::new();
    static EMPTY: AtomicU64 = AtomicU64::new(0);
    static CALLS: AtomicU64 = AtomicU64::new(0);
    static JOBS: AtomicU64 = AtomicU64::new(0);

    pub fn on() -> bool {
        static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        *ON.get_or_init(|| std::env::var_os("SOLID_ENGINE_DISPATCH_TRACE").is_some())
    }
    pub fn record(sent: Instant, start: Instant, end: Instant, got: usize) {
        QUEUE.record(start.saturating_duration_since(sent));
        EXEC.record(end.saturating_duration_since(start));
        TOTAL.record(end.saturating_duration_since(sent));
        CALLS.fetch_add(1, Ordering::Relaxed);
        JOBS.fetch_add(got as u64, Ordering::Relaxed);
        if got == 0 {
            EMPTY.fetch_add(1, Ordering::Relaxed);
        }
    }
    pub fn report() -> String {
        format!(
            "claim_calls={} claim_empty={} claim_jobs={} queue_wait_us[{}] claim_exec_us[{}] claim_total_us[{}]",
            CALLS.load(Ordering::Relaxed),
            EMPTY.load(Ordering::Relaxed),
            JOBS.load(Ordering::Relaxed),
            QUEUE.report(),
            EXEC.report(),
            TOTAL.report()
        )
    }
}

/// Diagnostic du WAL (SOLID_ENGINE_DISPATCH_TRACE) : profondeur de file, attente d'envoi du shard, écriture, fsync.
pub mod wal_trace {
    use super::claim_trace::Hist;
    pub use super::claim_trace::on;
    use super::*;
    pub static DEPTH: AtomicU64 = AtomicU64::new(0);
    static DEPTH_MAX: AtomicU64 = AtomicU64::new(0);
    static DEPTH_SUM: AtomicU64 = AtomicU64::new(0);
    static DEPTH_N: AtomicU64 = AtomicU64::new(0);
    static SEND: Hist = Hist::new();
    static APPEND: Hist = Hist::new();
    static SYNC: Hist = Hist::new();
    static SNAP_ENC: Hist = Hist::new();
    static SNAP_IO: Hist = Hist::new();
    static SNAP: Hist = Hist::new();
    static SNAP_NS: AtomicU64 = AtomicU64::new(0);
    static SYNCS: AtomicU64 = AtomicU64::new(0);
    static RECORDS: AtomicU64 = AtomicU64::new(0);
    static PENDING_MAX: AtomicU64 = AtomicU64::new(0);

    pub fn before_send() {
        let d = DEPTH.fetch_add(1, Ordering::Relaxed) + 1;
        DEPTH_MAX.fetch_max(d, Ordering::Relaxed);
    }
    pub fn sent(wait: Duration) {
        SEND.record(wait);
    }
    pub fn received(records: usize) {
        let d = DEPTH.fetch_sub(1, Ordering::Relaxed);
        DEPTH_SUM.fetch_add(d, Ordering::Relaxed);
        DEPTH_N.fetch_add(1, Ordering::Relaxed);
        RECORDS.fetch_add(records as u64, Ordering::Relaxed);
    }
    pub fn snapshot(d: Duration) {
        SNAP.record(d);
        SNAP_NS.fetch_add(d.as_nanos() as u64, Ordering::Relaxed);
    }
    pub fn snapshot_encode(d: Duration) {
        SNAP_ENC.record(d);
    }
    pub fn snapshot_io(d: Duration) {
        SNAP_IO.record(d);
    }
    pub fn appended(d: Duration) {
        APPEND.record(d);
    }
    pub fn synced(d: Duration, confirmations: usize) {
        SYNC.record(d);
        SYNCS.fetch_add(1, Ordering::Relaxed);
        PENDING_MAX.fetch_max(confirmations as u64, Ordering::Relaxed);
    }
    pub fn report() -> String {
        let n = DEPTH_N.load(Ordering::Relaxed).max(1);
        format!(
            "wal_msgs={} wal_records={} wal_depth_mean={:.1} wal_depth_max={} wal_syncs={} wal_sync_batch_max={} wal_send_wait_us[{}] wal_append_us[{}] wal_sync_us[{}] snapshot_total_ms={} snapshot_us[{}] snap_encode_us[{}] snap_io_us[{}]",
            DEPTH_N.load(Ordering::Relaxed),
            RECORDS.load(Ordering::Relaxed),
            DEPTH_SUM.load(Ordering::Relaxed) as f64 / n as f64,
            DEPTH_MAX.load(Ordering::Relaxed),
            SYNCS.load(Ordering::Relaxed),
            PENDING_MAX.load(Ordering::Relaxed),
            SEND.report(),
            APPEND.report(),
            SYNC.report(),
            SNAP_NS.load(Ordering::Relaxed) / 1_000_000,
            SNAP.report(),
            SNAP_ENC.report(),
            SNAP_IO.report()
        )
    }
}

/// Temps du thread shard par phase (SOLID_ENGINE_DISPATCH_TRACE), mesuré par lot de commandes.
pub mod shard_phase {
    use super::*;
    pub use super::claim_trace::on;
    pub const WAIT: usize = 0;
    pub const APPLY: usize = 1;
    pub const FLUSH: usize = 2;
    pub const LOG: usize = 3;
    pub const REPLY: usize = 4;
    pub const ENC: usize = 5;
    pub const OUTSEND: usize = 6;
    pub const RELEASE: usize = 7;
    static NS: [AtomicU64; 8] = [const { AtomicU64::new(0) }; 8];
    static CMDS: AtomicU64 = AtomicU64::new(0);
    static BATCHES: AtomicU64 = AtomicU64::new(0);
    pub fn add(phase: usize, d: Duration) {
        NS[phase].fetch_add(d.as_nanos() as u64, Ordering::Relaxed);
    }
    pub fn count(cmds: u64) {
        CMDS.fetch_add(cmds, Ordering::Relaxed);
        BATCHES.fetch_add(1, Ordering::Relaxed);
    }
    pub fn report() -> String {
        let ms = |i: usize| NS[i].load(Ordering::Relaxed) / 1_000_000;
        let cmds = CMDS.load(Ordering::Relaxed).max(1);
        format!(
            "shard_wait_ms={} shard_apply_ms={} shard_flush_ms={} flush_log_ms={} flush_reply_ms={} cb_encode_ms={} cb_outsend_ms={} cb_release_ms={} shard_cmds={} shard_batches={} apply_ns_per_cmd={} flush_ns_per_cmd={}",
            ms(WAIT),
            ms(APPLY),
            ms(FLUSH),
            ms(LOG),
            ms(REPLY),
            ms(ENC),
            ms(OUTSEND),
            ms(RELEASE),
            cmds,
            BATCHES.load(Ordering::Relaxed),
            NS[APPLY].load(Ordering::Relaxed) / cmds,
            NS[FLUSH].load(Ordering::Relaxed) / cmds
        )
    }
}
