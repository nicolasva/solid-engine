//! Réacteur TCP expérimental (Linux, `SOLID_ENGINE_REACTORS=N`) : N threads multiplexent toutes les
//! connexions avec epoll, à la place d'un thread lecteur et d'un thread écrivain par connexion.
//! Même protocole, mêmes shards, même sémantique ; les callbacks des shards déposent leurs réponses
//! dans une file par réacteur, réveillé par un eventfd seulement quand la file était vide.
//! Limites du prototype : pas de délai d'inactivité, requêtes rares traitées dans un thread dédié.

use crate::engine::Engine;
use solid_engine_core::*;
use solid_engine_protocol::{read_frame, write_frame, Frame, ProtocolError, Request, Response, HEADER_LEN};
use std::cell::Cell;
use std::collections::HashMap;
use std::io::{ErrorKind, Read, Write};
use std::net::TcpStream;
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
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
}

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
            drain_us: std::env::var("SOLID_ENGINE_REACTOR_DRAIN_US").ok().and_then(|v| v.parse().ok()).unwrap_or(0),
        };
        threads.push(std::thread::Builder::new().name("se-reactor".into()).spawn(move || r.run()).unwrap());
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
                        c.inflight = c.inflight.saturating_sub(1);
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

    fn direct_flush(&self) {
        let mut d = self.direct.as_ref().unwrap().borrow_mut();
        let Direct { shard, durable, failed, pending, waiters, mailbox, .. } = &mut *d;
        let now = now_ms();
        mailbox.apply(shard, pending, failed, now);
        shard.tick(now);
        if !waiters.is_empty() {
            self.serve_waiters(shard, pending, failed, waiters, now);
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
                        Ok(Some(job)) => Some(Response::Claim(Some(job))),
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

fn to_response(r: Result<Response>) -> Response {
    r.unwrap_or_else(Response::Error)
}
