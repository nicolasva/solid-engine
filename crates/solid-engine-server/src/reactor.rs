//! Réacteur TCP expérimental (Linux, `SOLID_ENGINE_REACTORS=N`) : N threads multiplexent toutes les
//! connexions avec epoll, à la place d'un thread lecteur et d'un thread écrivain par connexion.
//! Même protocole, mêmes shards, même sémantique ; les callbacks des shards déposent leurs réponses
//! dans une file par réacteur, réveillé par un eventfd seulement quand la file était vide.
//! Limites du prototype : pas de délai d'inactivité, requêtes rares traitées dans un thread dédié.

use crate::engine::Engine;
use solid_engine_core::*;
use solid_engine_protocol::{msg, read_frame, write_frame, Frame, ProtocolError, Request, Response, HEADER_LEN};
use std::cell::Cell;
use std::collections::HashMap;
use std::io::{ErrorKind, Read, Write};
use std::net::TcpStream;
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

const MAX_IN_FLIGHT: usize = 256;
const BODY_OVERHEAD: usize = 4096;
const CRC_LEN: usize = 4;
const EPOLLIN: u32 = 1;
const EPOLLOUT: u32 = 4;
const EPOLLERR: u32 = 8;
const EPOLLHUP: u32 = 0x10;
const CTL_ADD: i32 = 1;
const CTL_DEL: i32 = 2;
const CTL_MOD: i32 = 3;
const WAKE_ID: u64 = u64::MAX;

#[cfg_attr(target_arch = "x86_64", repr(C, packed))]
#[cfg_attr(not(target_arch = "x86_64"), repr(C))]
#[derive(Clone, Copy)]
struct EpollEvent {
    events: u32,
    data: u64,
}

thread_local! {
    /// SOLID_ENGINE_MEM_STATS : diagnostic mémoire périodique (activé, dernier affichage).
    static MEM_STATS: std::cell::Cell<(bool, std::time::Instant)> = std::cell::Cell::new((std::env::var("SOLID_ENGINE_MEM_STATS").is_ok(), std::time::Instant::now()));
}

extern "C" {
    fn epoll_create1(flags: i32) -> i32;
    fn epoll_ctl(epfd: i32, op: i32, fd: i32, ev: *mut EpollEvent) -> i32;
    fn epoll_wait(epfd: i32, events: *mut EpollEvent, max: i32, timeout: i32) -> i32;
    fn eventfd(init: u32, flags: i32) -> i32;
    fn read(fd: i32, buf: *mut std::ffi::c_void, n: usize) -> isize;
    fn write(fd: i32, buf: *const std::ffi::c_void, n: usize) -> isize;
}

pub fn reactor_count() -> usize {
    std::env::var("SOLID_ENGINE_REACTORS").ok().and_then(|v| v.parse().ok()).unwrap_or(0)
}

struct Shared {
    efd: i32,
    responses: Mutex<Vec<(u64, Frame)>>,
    new_conns: Mutex<Vec<(u64, Stream, bool)>>,
}

impl Shared {
    fn wake(&self) {
        let one = 1u64.to_ne_bytes();
        unsafe { write(self.efd, one.as_ptr().cast(), 8) };
    }

    fn respond(&self, conn: u64, frame: Frame) {
        let mut q = self.responses.lock().unwrap();
        let was_empty = q.is_empty();
        q.push((conn, frame));
        drop(q);
        if was_empty && IN_REACTOR_FLUSH.with(|f| f.get()) != self as *const Shared as usize {
            self.wake();
        }
    }
}

/// Profil optionnel (SOLID_ENGINE_REACTOR_STATS=fichier) : CPU thread par segment exclusif et compteurs.
/// ns : 0 epoll_wait, 1 read, 2 décodage, 3 soumission aux shards, 4 récupération et encodage des réponses,
/// 5 write, 6 epoll_ctl. n : 0 réveils, 1 événements, 2 reads, 3 octets lus, 4 trames, 5 lots de réponses,
/// 6 réponses, 7 writes, 8 octets écrits, 9 epoll_ctl.
struct Prof {
    on: bool,
    path: String,
    ns: [Cell<u64>; 7],
    n: [Cell<u64>; 10],
    last: Cell<std::time::Instant>,
}

impl Prof {
    fn new() -> Self {
        let path = std::env::var("SOLID_ENGINE_REACTOR_STATS").unwrap_or_default();
        Self { on: !path.is_empty(), path, ns: Default::default(), n: Default::default(), last: Cell::new(std::time::Instant::now()) }
    }

    fn now(&self) -> u64 {
        if self.on { crate::stats::thread_cpu_ns() } else { 0 }
    }

    fn add(&self, i: usize, t0: u64) {
        if self.on {
            self.ns[i].set(self.ns[i].get() + crate::stats::thread_cpu_ns().saturating_sub(t0));
        }
    }

    fn cnt(&self, i: usize, v: u64) {
        if self.on {
            self.n[i].set(self.n[i].get() + v);
        }
    }

    fn publish(&self) {
        if self.on && self.last.get().elapsed() >= std::time::Duration::from_millis(500) {
            self.last.set(std::time::Instant::now());
            let all: Vec<String> = self.ns.iter().chain(self.n.iter()).map(|c| c.get().to_string()).collect();
            let _ = std::fs::write(&self.path, all.join(" "));
        }
    }
}

struct Conn {
    stream: Stream,
    inbuf: Vec<u8>,
    pos: usize,
    out: Vec<u8>,
    out_pos: usize,
    inflight: usize,
    interest: u32,
    closing: bool,
    last_push_shard: Option<usize>,
    claims: u32,
    authed: bool,
    sub: Option<Sub>,
}

/// Abonnement Hyperlane d'une connexion : le réacteur pousse des jobs dans la limite des crédits.
struct Sub {
    channel_id: u16,
    lease_ms: u32,
    rid: u64,
    st: Arc<SubState>,
}

/// État partagé avec les callbacks des autres shards (réservations distantes).
struct SubState {
    credits: AtomicU64,
    remote_busy: AtomicBool,
    remote_empty: AtomicBool,
    remote_gen: AtomicU64,
    retry_at: AtomicU64,
}

const DISPATCH_MAX: u64 = 256;
const DISPATCH_MAX_BYTES: usize = 256 * 1024;
const REMOTE_RETRY_MS: u64 = 50;

struct Reactor {
    ep: i32,
    shared: Arc<Shared>,
    engine: Arc<Engine>,
    conns: HashMap<u64, Conn>,
    scratch: Vec<u8>,
    max_body: usize,
    affinity: bool,
    live: Arc<AtomicUsize>,
    auth: Option<Arc<Vec<u8>>>,
    prof: Prof,
    drain_us: u64,
    /// Prototype Zero-Dispatch (SOLID_ENGINE_REACTOR_DIRECT=1, Memory) : le réacteur possède son propre shard.
    direct: Option<std::cell::RefCell<Direct>>,
    /// Mode direct avec plusieurs shards : les commandes d'un autre shard passent par sa boîte aux lettres.
    multi: bool,
    /// Connexions abonnées (tourniquet pour l'équité) et drapeau de réveil par le notifier du moteur.
    subs: std::cell::RefCell<Vec<u64>>,
    sub_rr: usize,
    hungry: Arc<AtomicBool>,
}

/// Transport d'une connexion : TCP ou socket Unix (même traitement des commandes).
pub enum Stream {
    Tcp(TcpStream),
    Unix(UnixStream),
}

impl From<TcpStream> for Stream {
    fn from(s: TcpStream) -> Self {
        Stream::Tcp(s)
    }
}

impl From<UnixStream> for Stream {
    fn from(s: UnixStream) -> Self {
        Stream::Unix(s)
    }
}

impl AsRawFd for Stream {
    fn as_raw_fd(&self) -> RawFd {
        match self {
            Stream::Tcp(s) => s.as_raw_fd(),
            Stream::Unix(s) => s.as_raw_fd(),
        }
    }
}

impl Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Stream::Tcp(s) => s.read(buf),
            Stream::Unix(s) => s.read(buf),
        }
    }
}

impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Stream::Tcp(s) => s.write(buf),
            Stream::Unix(s) => s.write(buf),
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(Clone)]
pub struct Handle {
    shared: Arc<Shared>,
    next_id: Arc<AtomicU64>,
}

/// Démarre `n` réacteurs ; renvoie leurs poignées (pour répartir les connexions) et leurs threads.
pub fn start(
    engine: &Arc<Engine>,
    live: &Arc<AtomicUsize>,
    n: usize,
    auth: Option<Arc<Vec<u8>>>,
) -> (Vec<Handle>, Vec<std::thread::JoinHandle<()>>) {
    let mut handles = Vec::new();
    let mut threads = Vec::new();
    for index in 0..n {
        let ep = unsafe { epoll_create1(0x80000) };
        let efd = unsafe { eventfd(0, 0x800 | 0x80000) };
        assert!(ep >= 0 && efd >= 0, "epoll/eventfd indisponibles");
        let shared = Arc::new(Shared { efd, responses: Mutex::new(Vec::new()), new_conns: Mutex::new(Vec::new()) });
        let mut ev = EpollEvent { events: EPOLLIN, data: WAKE_ID };
        unsafe { epoll_ctl(ep, CTL_ADD, efd, &mut ev) };
        let hungry = Arc::new(AtomicBool::new(false));
        if crate::engine::direct_enabled() && engine.shard_count() > 1 {
            engine.register_waker(hungry.clone(), efd);
        }
        let r = Reactor {
            ep,
            shared: shared.clone(),
            engine: engine.clone(),
            conns: HashMap::new(),
            scratch: vec![0u8; 64 * 1024],
            max_body: engine.config().limits.max_payload_bytes + BODY_OVERHEAD,
            affinity: crate::engine::claim_affinity(),
            live: live.clone(),
            auth: auth.clone(),
            prof: Prof::new(),
            direct: direct_shard(engine, n, index, &shared),
            multi: engine.shard_count() > 1,
            subs: Default::default(),
            sub_rr: 0,
            hungry,
            drain_us: std::env::var("SOLID_ENGINE_REACTOR_DRAIN_US").ok().and_then(|v| v.parse().ok()).unwrap_or(0),
        };
        threads.push(
            std::thread::Builder::new()
                .name("se-reactor".into())
                .spawn(move || {
                    pin_to_core(index);
                    r.run()
                })
                .unwrap(),
        );
        handles.push(Handle { shared, next_id: Arc::new(AtomicU64::new(0)) });
    }
    (handles, threads)
}

impl Handle {
    /// `needs_auth` : la connexion doit s'authentifier (AUTH) avant toute autre commande.
    pub fn add(&self, stream: impl Into<Stream>, needs_auth: bool) {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        self.shared.new_conns.lock().unwrap().push((id, stream.into(), needs_auth));
        self.shared.wake();
    }
}

impl Reactor {
    fn run(mut self) {
        let mut events = vec![EpollEvent { events: 0, data: 0 }; 256];
        let mut touched: Vec<u64> = Vec::new();
        while !self.engine.is_shutting_down() {
            let t = self.prof.now();
            let timeout = self.direct_timeout_ms();
            let n = unsafe { epoll_wait(self.ep, events.as_mut_ptr(), events.len() as i32, timeout) };
            self.prof.add(0, t);
            if n < 0 {
                continue;
            }
            if n > 0 {
                self.prof.cnt(0, 1);
                self.prof.cnt(1, n as u64);
            }
            self.prof.publish();
            let mut woken = false;
            for ev in &events[..n as usize] {
                let (flags, id) = (ev.events, ev.data);
                if id == WAKE_ID {
                    woken = true;
                    if self.direct.is_some() {
                        // Lu avant de vider la boîte aux lettres : un envoi postérieur réécrit l'eventfd.
                        let mut buf = [0u8; 8];
                        unsafe { read(self.shared.efd, buf.as_mut_ptr().cast(), 8) };
                    }
                    continue;
                }
                if flags & (EPOLLERR | EPOLLHUP) != 0 && flags & EPOLLIN == 0 {
                    self.close(id);
                    continue;
                }
                if flags & EPOLLIN != 0 {
                    self.on_readable(id);
                }
                self.service(id);
            }
            if self.direct.is_some() {
                self.direct_flush();
                woken |= !self.shared.responses.lock().unwrap().is_empty();
            }
            if woken {
                let t = self.prof.now();
                if self.direct.is_none() {
                    let mut buf = [0u8; 8];
                    unsafe { read(self.shared.efd, buf.as_mut_ptr().cast(), 8) };
                }
                if self.drain_us > 0 {
                    std::thread::sleep(std::time::Duration::from_micros(self.drain_us));
                }
                self.adopt();
                let batch = std::mem::take(&mut *self.shared.responses.lock().unwrap());
                touched.clear();
                self.prof.cnt(6, batch.len() as u64);
                for (cid, frame) in batch {
                    if let Some(c) = self.conns.get_mut(&cid) {
                        let _ = write_frame(&mut c.out, &frame);
                        if frame.msg_type != msg::DISPATCH {
                            c.inflight = c.inflight.saturating_sub(1);
                        }
                        if touched.last() != Some(&cid) {
                            touched.push(cid);
                        }
                    }
                }
                self.prof.cnt(5, 1);
                self.prof.add(4, t);
                touched.sort_unstable();
                touched.dedup();
                for cid in touched.drain(..) {
                    self.process_input(cid);
                    self.service(cid);
                }
            }
        }
        if self.direct.is_some() {
            self.direct_flush();
            if let Some(d) = self.direct.take() {
                if let Some(dur) = d.into_inner().durable {
                    crate::engine::close_durable(dur);
                }
            }
        }
    }

    fn direct_timeout_ms(&self) -> i32 {
        match &self.direct {
            Some(d) => {
                let d = d.borrow();
                if !d.pending.is_empty() || !self.shared.responses.lock().unwrap().is_empty() {
                    return 0;
                }
                let wake = d.waiters.iter().map(|w| w.deadline).chain(d.shard.next_deadline()).min();
                wake.map_or(100, |t| t.saturating_sub(now_ms()).min(100) as i32)
            }
            None => 100,
        }
    }

    fn direct_flush(&mut self) {
        let mut d = self.direct.as_ref().unwrap().borrow_mut();
        let index = d.index;
        let Direct { shard, durable, failed, pending, waiters, mailbox, .. } = &mut *d;
        let now = now_ms();
        mailbox.apply(shard, pending, failed, now);
        shard.tick(now);
        if shard.take_shrink_finished() {
            crate::stats::trim_memory();
        }
        MEM_STATS.with(|m| {
            let (on, last) = m.get();
            if on && last.elapsed() >= crate::stats::mem_stats_interval() {
                m.set((on, std::time::Instant::now()));
                eprintln!("mem_report thread={:?} {:?}", std::thread::current().name(), shard.mem_report());
                eprintln!("mem_process {}", crate::stats::process_memory());
            }
        });
        if !waiters.is_empty() {
            self.serve_waiters(shard, pending, failed, waiters, now);
        }
        if !self.subs.get_mut().is_empty() {
            let (shared, engine, multi) = (&self.shared, &self.engine, self.multi);
            dispatch_subs(
                &mut self.conns,
                self.subs.get_mut(),
                &mut self.sub_rr,
                &self.hungry,
                (shared, engine, multi, index),
                shard,
                pending,
                failed,
                now,
            );
        }
        IN_REACTOR_FLUSH.with(|f| f.set(Arc::as_ptr(&self.shared) as usize));
        crate::engine::flush(shard, durable, failed, pending, self.engine.config());
        IN_REACTOR_FLUSH.with(|f| f.set(0));
        if shard.take_ready_flag() && self.multi {
            self.engine.notify_ready();
        }
    }

    fn serve_waiters(
        &self,
        shard: &mut solid_engine_core::shard::Shard,
        pending: &mut crate::engine::Pending,
        failed: &Option<EngineError>,
        waiters: &mut Vec<Waiter>,
        now: u64,
    ) {
        let shutting = self.engine.is_shutting_down();
        let mut i = 0;
        while i < waiters.len() {
            let w = &waiters[i];
            let outcome = if let Some(e) = failed {
                Some(Response::Error(e.clone()))
            } else if shutting {
                Some(Response::Error(EngineError::new(ErrorCode::ShuttingDown, "server shutting down")))
            } else {
                match w.lease_ms {
                    Some(lease) => match shard.claim(w.channel_id, lease, now) {
                        Ok(Some(job)) if w.ty == solid_engine_protocol::msg::CLAIM_BATCH => Some(Response::ClaimBatch(vec![job])),
                        Ok(Some(job)) => Some(Response::Claim(Some(job))),
                        Ok(None) if now >= w.deadline && w.ty == solid_engine_protocol::msg::CLAIM_BATCH => Some(Response::ClaimBatch(Vec::new())),
                        Ok(None) if now >= w.deadline => Some(Response::Claim(None)),
                        Ok(None) => None,
                        Err(e) => Some(Response::Error(e)),
                    },
                    None => {
                        let available = shard.stats(Some(w.channel_id), now).ready > 0;
                        (available || now >= w.deadline).then_some(Response::Wait { available })
                    }
                }
            };
            match outcome {
                Some(resp) => {
                    let w = waiters.remove(i);
                    let shared = self.shared.clone();
                    pending.push(Box::new(move |failure| {
                        let resp = match failure {
                            Some(e) => Response::Error(e.clone()),
                            None => resp,
                        };
                        shared.respond(w.conn, resp.to_frame(w.ty, w.rid));
                    }));
                }
                None => i += 1,
            }
        }
    }

    fn adopt(&mut self) {
        let fresh = std::mem::take(&mut *self.shared.new_conns.lock().unwrap());
        for (id, stream, needs_auth) in fresh {
            let mut ev = EpollEvent { events: EPOLLIN, data: id };
            if unsafe { epoll_ctl(self.ep, CTL_ADD, stream.as_raw_fd(), &mut ev) } != 0 {
                self.live.fetch_sub(1, Ordering::SeqCst);
                continue;
            }
            self.conns.insert(
                id,
                Conn {
                    stream,
                    inbuf: Vec::new(),
                    pos: 0,
                    out: Vec::new(),
                    out_pos: 0,
                    inflight: 0,
                    interest: EPOLLIN,
                    closing: false,
                    last_push_shard: None,
                    claims: 0,
                    authed: !needs_auth,
                    sub: None,
                },
            );
        }
    }

    fn close(&mut self, id: u64) {
        if let Some(c) = self.conns.remove(&id) {
            unsafe { epoll_ctl(self.ep, CTL_DEL, c.stream.as_raw_fd(), std::ptr::null_mut()) };
            self.live.fetch_sub(1, Ordering::SeqCst);
            if let Some(d) = &self.direct {
                d.borrow_mut().waiters.retain(|w| w.conn != id);
            }
            if c.sub.is_some() {
                self.subs.get_mut().retain(|&s| s != id);
            }
        }
    }

    fn on_readable(&mut self, id: u64) {
        let Some(c) = self.conns.get_mut(&id) else { return };
        if c.closing || c.inflight >= MAX_IN_FLIGHT {
            return;
        }
        let t = self.prof.now();
        let got = c.stream.read(&mut self.scratch);
        self.prof.add(1, t);
        self.prof.cnt(2, 1);
        match got {
            Ok(0) => {
                c.closing = true;
                c.pos = c.inbuf.len();
            }
            Ok(n) => {
                self.prof.cnt(3, n as u64);
                c.inbuf.extend_from_slice(&self.scratch[..n])
            }
            Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::Interrupted) => {}
            Err(_) => {
                self.close(id);
                return;
            }
        }
        self.process_input(id);
    }

    /// Décode les trames complètes tant que la fenêtre de requêtes en vol n'est pas pleine.
    fn process_input(&mut self, id: u64) {
        let Some(mut c) = self.conns.remove(&id) else { return };
        while !c.closing && c.inflight < MAX_IN_FLIGHT {
            let avail = &c.inbuf[c.pos..];
            if avail.len() < HEADER_LEN {
                break;
            }
            let len = u32::from_le_bytes(avail[16..20].try_into().unwrap()) as usize;
            if len > self.max_body {
                self.reject(&mut c, ProtocolError::TooLarge(len as u32));
                break;
            }
            let total = HEADER_LEN + len + CRC_LEN;
            if avail.len() < total {
                break;
            }
            let mut slice = &avail[..total];
            let t = self.prof.now();
            let frame = match read_frame(&mut slice, self.max_body) {
                Ok(f) => f,
                Err(e) => {
                    self.reject(&mut c, e);
                    break;
                }
            };
            self.prof.add(2, t);
            self.prof.cnt(4, 1);
            c.pos += total;
            self.dispatch(id, &mut c, frame);
        }
        if c.pos == c.inbuf.len() {
            c.inbuf.clear();
            c.pos = 0;
        } else if c.pos > 64 * 1024 {
            c.inbuf.drain(..c.pos);
            c.pos = 0;
        }
        self.conns.insert(id, c);
    }

    fn reject(&self, c: &mut Conn, e: ProtocolError) {
        let resp = Response::Error(EngineError::new(ErrorCode::BadRequest, e.to_string()));
        let _ = write_frame(&mut c.out, &resp.to_frame(0, 0));
        c.closing = true;
    }

    fn dispatch(&self, id: u64, c: &mut Conn, frame: Frame) {
        let (ty, rid) = (frame.msg_type, frame.request_id);
        let (engine, shared) = (&self.engine, &self.shared);
        macro_rules! submit {
            ($call:expr, $map:expr) => {{
                c.inflight += 1;
                let shared = shared.clone();
                $call(move |result: Result<_>| {
                    shared.respond(id, to_response(result.map($map)).to_frame(ty, rid));
                })
            }};
        }
        let t = self.prof.now();
        let decoded = Request::decode(ty, &frame.body);
        self.prof.add(2, t);
        if let (false, Some(expected), Ok(req)) = (c.authed, &self.auth, &decoded) {
            let (resp, close) = crate::net::auth_step(expected, &mut c.authed, req);
            let _ = write_frame(&mut c.out, &resp.to_frame(ty, rid));
            c.closing |= close;
            return;
        }
        if let (Some(_), Ok(req)) = (&self.direct, &decoded) {
            match req {
                Request::Subscribe { channel_id, lease_ms, credits } => {
                    let resp = if c.sub.is_some() {
                        Response::Error(EngineError::new(ErrorCode::BadRequest, "already subscribed"))
                    } else {
                        c.sub = Some(Sub {
                            channel_id: *channel_id,
                            lease_ms: *lease_ms,
                            rid,
                            st: Arc::new(SubState {
                                credits: AtomicU64::new(*credits as u64),
                                remote_busy: AtomicBool::new(false),
                                remote_empty: AtomicBool::new(false),
                                remote_gen: AtomicU64::new(0),
                                retry_at: AtomicU64::new(0),
                            }),
                        });
                        self.subs.borrow_mut().push(id);
                        Response::Subscribed
                    };
                    let _ = write_frame(&mut c.out, &resp.to_frame(ty, rid));
                    return;
                }
                Request::Credit { credits } => {
                    match &c.sub {
                        Some(s) => {
                            s.st.credits.fetch_add(*credits as u64, Ordering::AcqRel);
                        }
                        None => {
                            let e = EngineError::new(ErrorCode::BadRequest, "not subscribed");
                            let _ = write_frame(&mut c.out, &Response::Error(e).to_frame(ty, rid));
                        }
                    }
                    return;
                }
                _ => {}
            }
        }
        let t = self.prof.now();
        if let (Some(d), Ok(req), false) = (&self.direct, &decoded, self.multi) {
            let mut d = d.borrow_mut();
            let Direct { shard, pending, next_id, failed, waiters, .. } = &mut *d;
            let now = now_ms();
            let resp = match req.clone() {
                Request::Ping => Response::Pong,
                Request::Auth(_) => Response::Authed,
                Request::PushJob(mut p) => {
                    if p.job_id == 0 {
                        *next_id += 1;
                        p.job_id = *next_id;
                    }
                    to_response(shard.push(p, now).map(Response::Push))
                }
                Request::PushBatch(ps) => match crate::net::push_batch_too_large(ps.len()) {
                    Some(e) => Response::Error(e),
                    None => Response::PushBatch(
                        ps.into_iter()
                            .map(|mut p| {
                                if p.job_id == 0 {
                                    *next_id += 1;
                                    p.job_id = *next_id;
                                }
                                shard.push(p, now)
                            })
                            .collect(),
                    ),
                },
                Request::AckJob(r) => to_response(shard.ack(r, now).map(|_| Response::Ack)),
                Request::NackJob { reservation, kind } => to_response(shard.nack(reservation, kind, now).map(|_| Response::Nack)),
                Request::ExtendLease { reservation, lease_ms } => {
                    to_response(shard.extend_lease(reservation, lease_ms, now).map(|lease_deadline| Response::ExtendLease { lease_deadline }))
                }
                Request::CancelJob { job_id } => to_response(shard.cancel(job_id, now).map(|_| Response::Cancel)),
                Request::GetJob { job_id } => match failed {
                    Some(e) => Response::Error(e.clone()),
                    None => to_response(shard.get(job_id, now).map(Response::Job)),
                },
                Request::GetQueueStats { channel_id } => match failed {
                    Some(e) => Response::Error(e.clone()),
                    None => Response::Stats(shard.stats(channel_id, now)),
                },
                Request::ClaimNextJob { channel_id, lease_ms, wait_ms } => match shard.claim(channel_id, lease_ms, now) {
                    Ok(None) if wait_ms > 0 => {
                        c.inflight += 1;
                        let deadline = now + wait_ms as u64;
                        waiters.push(Waiter { conn: id, ty, rid, channel_id, lease_ms: Some(lease_ms), deadline });
                        self.prof.add(3, t);
                        return;
                    }
                    other => to_response(other.map(Response::Claim)),
                },
                Request::WaitForJob { channel_id, timeout_ms } => {
                    let available = shard.stats(Some(channel_id), now).ready > 0;
                    if !available && timeout_ms > 0 {
                        c.inflight += 1;
                        waiters.push(Waiter { conn: id, ty, rid, channel_id, lease_ms: None, deadline: now + timeout_ms as u64 });
                        self.prof.add(3, t);
                        return;
                    }
                    Response::Wait { available }
                }
                Request::ClaimBatch { channel_id, lease_ms, max, wait_ms } => {
                    let max = max.min(crate::net::MAX_BATCH) as usize;
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
                    match error {
                        Some(e) if jobs.is_empty() => Response::Error(e),
                        _ if jobs.is_empty() && max > 0 && wait_ms > 0 => {
                            c.inflight += 1;
                            let deadline = now + wait_ms as u64;
                            waiters.push(Waiter { conn: id, ty, rid, channel_id, lease_ms: Some(lease_ms), deadline });
                            self.prof.add(3, t);
                            return;
                        }
                        _ => Response::ClaimBatch(jobs),
                    }
                }
                Request::AckBatch(rs) => {
                    let n = rs.len().min(crate::net::MAX_BATCH as usize);
                    Response::AckBatch(rs.into_iter().take(n).map(|r| shard.ack(r, now).err()).collect())
                }
                Request::Subscribe { .. } | Request::Credit { .. } => {
                    Response::Error(EngineError::new(ErrorCode::BadRequest, "subscriptions are not supported by the epoll reactor"))
                }
            };
            c.inflight += 1;
            let shared = self.shared.clone();
            pending.push(Box::new(move |failure| {
                let resp = match failure {
                    Some(e) => Response::Error(e.clone()),
                    None => resp,
                };
                shared.respond(id, resp.to_frame(ty, rid));
            }));
            self.prof.add(3, t);
            return;
        }
        match decoded {
            Err(e) => {
                let resp = Response::Error(EngineError::new(ErrorCode::BadRequest, e.to_string()));
                let _ = write_frame(&mut c.out, &resp.to_frame(ty, rid));
            }
            Ok(Request::PushJob(mut p)) if self.direct.is_some() => {
                let shard = engine.push_shard(&mut p);
                if shard == self.index() {
                    self.run_local(c, id, ty, rid, |sh, now| Some(to_response(sh.push(p, now).map(Response::Push))));
                } else {
                    submit!(|d| engine.push_async(p, d), Response::Push);
                }
                c.last_push_shard = Some(shard);
            }
            Ok(Request::AckBatch(rs)) if self.direct.is_some() => self.ack_batch(c, id, ty, rid, rs),
            Ok(Request::AckJob(r)) if self.owns(&r) => {
                {
                self.run_local(c, id, ty, rid, |sh, now| Some(to_response(sh.ack(r, now).map(|_| Response::Ack))));
            }
            }
            Ok(Request::NackJob { reservation, kind }) if self.owns(&reservation) => {
                self.run_local(c, id, ty, rid, |sh, now| Some(to_response(sh.nack(reservation, kind, now).map(|_| Response::Nack))));
            }
            Ok(Request::ExtendLease { reservation, lease_ms }) if self.owns(&reservation) => {
                self.run_local(c, id, ty, rid, |sh, now| {
                    Some(to_response(sh.extend_lease(reservation, lease_ms, now).map(|lease_deadline| Response::ExtendLease { lease_deadline })))
                });
            }
            Ok(Request::PushJob(p)) => c.last_push_shard = Some(submit!(|d| engine.push_async(p, d), Response::Push)),
            Ok(Request::AckJob(r)) => submit!(|d| engine.ack_async(r, d), |_| Response::Ack),
            Ok(Request::NackJob { reservation, kind }) => {
                submit!(|d| engine.nack_async(reservation, kind, d), |_| Response::Nack)
            }
            Ok(Request::ExtendLease { reservation, lease_ms }) => submit!(
                |d| engine.extend_lease_async(reservation, lease_ms, d),
                |lease_deadline| Response::ExtendLease { lease_deadline }
            ),
            Ok(Request::ClaimNextJob { channel_id, lease_ms, wait_ms }) => {
                c.claims = c.claims.wrapping_add(1);
                // Un job local est servi sans changer de thread ; un tour sur 8 balaie d'abord les autres shards.
                if self.direct.is_some()
                    && c.claims % 8 != 0
                    && self.run_local(c, id, ty, rid, |sh, now| match sh.claim(channel_id, lease_ms, now) {
                        Ok(None) => None,
                        other => Some(to_response(other.map(Response::Claim))),
                    })
                {
                    self.prof.add(3, t);
                    return;
                }
                c.inflight += 1;
                let (e, shared) = (engine.clone(), shared.clone());
                let hint = c.last_push_shard.filter(|_| self.affinity && c.claims % 8 != 0);
                let done = move |result: Result<Option<_>>| match result {
                    Ok(None) if wait_ms > 0 => {
                        std::thread::spawn(move || {
                            let resp = to_response(e.claim(channel_id, lease_ms, wait_ms).map(Response::Claim));
                            shared.respond(id, resp.to_frame(ty, rid));
                        });
                    }
                    other => shared.respond(id, to_response(other.map(Response::Claim)).to_frame(ty, rid)),
                };
                match hint {
                    Some(start) => engine.try_claim_async_from(channel_id, lease_ms, start, done),
                    None => engine.try_claim_async(channel_id, lease_ms, done),
                }
            }
            Ok(req) => {
                // Requêtes rares et potentiellement bloquantes : hors du réacteur.
                c.inflight += 1;
                let (e, shared) = (engine.clone(), shared.clone());
                std::thread::spawn(move || {
                    shared.respond(id, to_response(crate::net::handle(&e, req)).to_frame(ty, rid));
                });
            }
        }
        self.prof.add(3, t);
    }

    /// ACK groupés en mode multi-shards : ceux du shard local sont appliqués sur place, les autres partent
    /// en une commande par shard ; une seule réponse, une fois chaque partie durable.
    fn ack_batch(&self, c: &mut Conn, id: u64, ty: u8, rid: u64, rs: Vec<Reservation>) {
        let n = rs.len().min(crate::net::MAX_BATCH as usize);
        let me = self.index();
        let (mut results, mut local, mut remote_idx, mut remote) = (vec![None; n], Vec::new(), Vec::new(), Vec::new());
        {
            let mut d = self.direct.as_ref().unwrap().borrow_mut();
            let now = now_ms();
            for (i, r) in rs.into_iter().take(n).enumerate() {
                if self.engine.reservation_shard(&r).is_ok_and(|s| s == me) {
                    results[i] = d.shard.ack(r, now).err();
                    local.push(i);
                } else {
                    remote_idx.push(i);
                    remote.push(r);
                }
            }
        }
        c.inflight += 1;
        let shared = self.shared.clone();
        let parts = usize::from(!local.is_empty()) + usize::from(!remote.is_empty());
        if parts <= 1 && remote.is_empty() {
            let mut d = self.direct.as_ref().unwrap().borrow_mut();
            d.pending.push(Box::new(move |failure| {
                let resp = match failure {
                    Some(e) => Response::AckBatch(vec![Some(e.clone()); n]),
                    None => Response::AckBatch(results),
                };
                shared.respond(id, resp.to_frame(ty, rid));
            }));
            return;
        }
        let join = Arc::new(Mutex::new((results, parts)));
        let finish = {
            let (join, shared) = (join.clone(), shared.clone());
            move |apply: Box<dyn FnOnce(&mut Vec<Option<EngineError>>)>| {
                let mut j = join.lock().unwrap();
                apply(&mut j.0);
                j.1 -= 1;
                if j.1 == 0 {
                    let results = std::mem::take(&mut j.0);
                    drop(j);
                    shared.respond(id, Response::AckBatch(results).to_frame(ty, rid));
                }
            }
        };
        let finish = Arc::new(Mutex::new(Some(finish)));
        if !local.is_empty() {
            let finish = finish.clone();
            let mut d = self.direct.as_ref().unwrap().borrow_mut();
            d.pending.push(Box::new(move |failure| {
                let failure = failure.cloned();
                let f = finish.lock().unwrap();
                f.as_ref().unwrap()(Box::new(move |res| {
                    if let Some(e) = failure {
                        for i in local {
                            res[i] = Some(e.clone());
                        }
                    }
                }));
            }));
        }
        let finish = finish.clone();
        self.engine.ack_many_async(remote, move |per_job| {
            let f = finish.lock().unwrap();
            f.as_ref().unwrap()(Box::new(move |res| {
                for (i, r) in remote_idx.into_iter().zip(per_job) {
                    res[i] = r;
                }
            }));
        });
    }

    fn index(&self) -> usize {
        self.direct.as_ref().map_or(0, |d| d.borrow().index)
    }

    fn owns(&self, r: &Reservation) -> bool {
        self.direct.is_some() && self.engine.reservation_shard(r).is_ok_and(|s| s == self.index())
    }

    /// Exécute `f` sur le shard local ; la réponse part au flush (après le fsync en strict).
    /// Renvoie faux, sans rien envoyer, si `f` renvoie `None`.
    fn run_local(
        &self,
        c: &mut Conn,
        id: u64,
        ty: u8,
        rid: u64,
        f: impl FnOnce(&mut solid_engine_core::shard::Shard, u64) -> Option<Response>,
    ) -> bool {
        let mut d = self.direct.as_ref().unwrap().borrow_mut();
        let Some(resp) = f(&mut d.shard, now_ms()) else { return false };
        c.inflight += 1;
        let shared = self.shared.clone();
        d.pending.push(Box::new(move |failure| {
            let resp = match failure {
                Some(e) => Response::Error(e.clone()),
                None => resp,
            };
            shared.respond(id, resp.to_frame(ty, rid));
        }));
        true
    }

    /// Écrit ce qui est prêt, ferme si nécessaire et ajuste l'intérêt epoll.
    fn service(&mut self, id: u64) {
        let Some(c) = self.conns.get_mut(&id) else { return };
        let mut dead = false;
        while c.out_pos < c.out.len() {
            let t = self.prof.now();
            let w = c.stream.write(&c.out[c.out_pos..]);
            self.prof.add(5, t);
            self.prof.cnt(7, 1);
            if let Ok(n) = &w {
                self.prof.cnt(8, *n as u64);
            }
            match w {
                Ok(0) => {
                    dead = true;
                    break;
                }
                Ok(n) => c.out_pos += n,
                Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(_) => {
                    dead = true;
                    break;
                }
            }
        }
        if c.out_pos == c.out.len() {
            c.out.clear();
            c.out_pos = 0;
        }
        if dead || (c.closing && c.out.is_empty() && c.inflight == 0) {
            self.close(id);
            return;
        }
        let mut want = 0;
        if !c.closing && c.inflight < MAX_IN_FLIGHT {
            want |= EPOLLIN;
        }
        if !c.out.is_empty() {
            want |= EPOLLOUT;
        }
        if want != c.interest {
            c.interest = want;
            let mut ev = EpollEvent { events: want, data: id };
            let t = self.prof.now();
            unsafe { epoll_ctl(self.ep, CTL_MOD, c.stream.as_raw_fd(), &mut ev) };
            self.prof.add(6, t);
            self.prof.cnt(9, 1);
        }
    }
}

struct Direct {
    shard: solid_engine_core::shard::Shard,
    durable: Option<crate::engine::Durable>,
    failed: Option<EngineError>,
    pending: crate::engine::Pending,
    next_id: u128,
    index: usize,
    mailbox: Arc<crate::engine::Mailbox>,
    waiters: Vec<Waiter>,
}

/// Requête bloquante en attente d'un job : réservation (`lease_ms` > 0 ) ou simple attente de disponibilité.
struct Waiter {
    conn: u64,
    ty: u8,
    rid: u64,
    channel_id: u16,
    lease_ms: Option<u32>,
    deadline: u64,
}

fn direct_shard(engine: &Arc<Engine>, reactors: usize, index: usize, shared: &Shared) -> Option<std::cell::RefCell<Direct>> {
    if !crate::engine::direct_enabled() {
        return None;
    }
    assert!(reactors == engine.shard_count(), "REACTOR_DIRECT : un réacteur par shard");
    let (shard, durable, mailbox) = engine.take_direct(index).expect("shard direct indisponible");
    mailbox.set_wake_fd(shared.efd);
    Some(std::cell::RefCell::new(Direct {
        shard,
        durable,
        failed: None,
        pending: Vec::new(),
        next_id: ((index as u128 + 1) << 100) | 1,
        index,
        mailbox,
        waiters: Vec::new(),
    }))
}

thread_local! {
    /// Adresse du `Shared` du réacteur pendant son `flush` : ses réponses sont relevées juste après, sans réveil.
    static IN_REACTOR_FLUSH: Cell<usize> = const { Cell::new(0) };
}

/// Envoie une trame `Dispatch` déjà encodée ; elle part après la durabilité.
fn queue_frame(body: solid_engine_protocol::DispatchBody, conn: u64, rid: u64, pending: &mut crate::engine::Pending, shared: &Arc<Shared>) {
    let frame = body.into_frame(rid);
    let shared = shared.clone();
    pending.push(Box::new(move |failure| {
        let frame = match failure {
            Some(e) => Response::Error(e.clone()).to_frame(msg::DISPATCH, rid),
            None => frame,
        };
        shared.respond(conn, frame);
    }));
}

/// Fusion : à chaque tour, chaque abonné reçoit des jobs du shard local (un seul passage par lot) ; si le
/// shard local est à sec et que d'autres shards existent, une réservation asynchrone groupée les sollicite.
#[allow(clippy::too_many_arguments)]
fn dispatch_subs(
    conns: &mut HashMap<u64, Conn>,
    subs: &mut [u64],
    rr: &mut usize,
    hungry: &Arc<AtomicBool>,
    (shared, engine, multi, index): (&Arc<Shared>, &Arc<Engine>, bool, usize),
    shard: &mut solid_engine_core::shard::Shard,
    pending: &mut crate::engine::Pending,
    failed: &Option<EngineError>,
    now: u64,
) {
    let len = subs.len();
    *rr = (*rr + 1) % len.max(1);
    for k in 0..len {
        let id = subs[(*rr + k) % len];
        let Some(c) = conns.get_mut(&id) else { continue };
        let Some(sub) = &c.sub else { continue };
        if c.closing {
            continue;
        }
        let (st, channel_id, lease_ms, rid) = (sub.st.clone(), sub.channel_id, sub.lease_ms, sub.rid);
        let credits = st.credits.load(Ordering::Acquire);
        if credits == 0 {
            continue;
        }
        if let Some(e) = failed {
            let frame = Response::Error(e.clone()).to_frame(msg::DISPATCH, rid);
            let _ = write_frame(&mut c.out, &frame);
            c.closing = true;
            continue;
        }
        let want = credits.min(DISPATCH_MAX) as usize;
        let mut got = 0u64;
        let mut body = solid_engine_protocol::DispatchBody::new();
        while (got as usize) < want {
            match shard.claim_with(channel_id, lease_ms, now, |m, p| body.push(m, p)) {
                Ok(Some(())) => got += 1,
                Ok(None) | Err(_) => break,
            }
            if body.len() >= DISPATCH_MAX_BYTES {
                queue_frame(std::mem::take(&mut body), id, rid, pending, shared);
            }
        }
        if got > 0 {
            st.credits.fetch_sub(got, Ordering::AcqRel);
            if !body.is_empty() {
                queue_frame(body, id, rid, pending, shared);
            }
        }
        let left = credits - got;
        if !multi || left == 0 || st.remote_busy.load(Ordering::Acquire) {
            continue;
        }
        let gen = engine.notify_ticks();
        let retry = !st.remote_empty.load(Ordering::Acquire) || st.remote_gen.load(Ordering::Acquire) != gen || now >= st.retry_at.load(Ordering::Acquire);
        if !retry {
            continue;
        }
        let rwant = left.min(DISPATCH_MAX);
        st.remote_busy.store(true, Ordering::Release);
        st.remote_gen.store(gen, Ordering::Release);
        // Armé avant la requête : un job prêt pendant la réservation réveille ce réacteur.
        hungry.store(true, Ordering::Release);
        st.credits.fetch_sub(rwant, Ordering::AcqRel);
        let (shared, st) = (shared.clone(), st.clone());
        engine.claim_many_async(channel_id, lease_ms, rwant as usize, index, move |res| {
            let (jobs, error) = match res {
                Ok(jobs) => (jobs, None),
                Err(e) => (Vec::new(), Some(e)),
            };
            st.credits.fetch_add(rwant - jobs.len() as u64, Ordering::AcqRel);
            st.remote_empty.store(jobs.is_empty(), Ordering::Release);
            let delay = if error.is_some() { 1000 } else { REMOTE_RETRY_MS };
            st.retry_at.store(now_ms() + delay, Ordering::Release);
            if let Some(e) = error {
                shared.respond(id, Response::Error(e).to_frame(msg::DISPATCH, rid));
            }
            let mut chunk = Vec::new();
            let mut bytes = 0usize;
            let mut jobs = jobs.into_iter().peekable();
            while let Some(job) = jobs.next() {
                bytes += job.payload.len() + 64;
                chunk.push(job);
                if bytes >= DISPATCH_MAX_BYTES || jobs.peek().is_none() {
                    shared.respond(id, Response::Dispatch(std::mem::take(&mut chunk)).to_frame(msg::DISPATCH, rid));
                    bytes = 0;
                }
            }
            st.remote_busy.store(false, Ordering::Release);
            shared.wake();
        });
    }
}

/// `SOLID_ENGINE_REACTOR_PIN=1` : épingle le réacteur `index` sur le cœur `index` (modulo les cœurs disponibles).
fn pin_to_core(index: usize) {
    if std::env::var_os("SOLID_ENGINE_REACTOR_PIN").is_none() {
        return;
    }
    extern "C" {
        fn sched_setaffinity(pid: i32, size: usize, mask: *const u64) -> i32;
    }
    let cpus = std::thread::available_parallelism().map_or(1, |n| n.get());
    let cpu = index % cpus;
    let mut mask = [0u64; 16];
    mask[cpu / 64] = 1 << (cpu % 64);
    unsafe { sched_setaffinity(0, std::mem::size_of_val(&mask), mask.as_ptr()) };
}

fn to_response(r: Result<Response>) -> Response {
    r.unwrap_or_else(Response::Error)
}
