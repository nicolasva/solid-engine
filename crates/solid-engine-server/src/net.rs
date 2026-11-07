//! Serveur réseau : sockets Unix et TCP, connexions persistantes, requêtes en vol multiples.

use crate::engine::Engine;
use solid_engine_core::config::Config;
use solid_engine_core::*;
use solid_engine_protocol::{
    msg, read_frame, Frame, ProtocolError, Request, Response,
};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixListener;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

/// Délai maximal avant de revérifier l'arrêt ; une connexion entrante réveille l'acceptation immédiatement.
const ACCEPT_POLL: Duration = Duration::from_millis(20);

#[repr(C)]
struct PollFd {
    fd: i32,
    events: i16,
    revents: i16,
}

extern "C" {
    fn poll(fds: *mut PollFd, nfds: u64, timeout: i32) -> i32;
}

/// Attend une connexion entrante sur le socket d'écoute (au plus `ACCEPT_POLL`) ; sinon dort après une erreur.
fn pause_after_accept_error(e: &std::io::Error, fd: std::os::fd::RawFd) {
    if e.kind() == std::io::ErrorKind::WouldBlock {
        let mut p = PollFd {
            fd,
            events: 1,
            revents: 0,
        };
        unsafe { poll(&mut p, 1, ACCEPT_POLL.as_millis() as i32) };
    } else {
        std::thread::sleep(ACCEPT_POLL);
    }
}
/// Marge pour l'enveloppe et les champs fixes autour du payload.
const BODY_OVERHEAD: usize = 4096;

pub struct Server {
    engine: Arc<Engine>,
    tcp_addr: Option<SocketAddr>,
    unix_path: Option<PathBuf>,
    accept_threads: Vec<JoinHandle<()>>,
}

impl Server {
    pub fn start(cfg: Config) -> std::io::Result<Self> {
        let server_cfg = cfg.server.clone();
        let token = resolve_token(&server_cfg)?;
        let direct_shards = crate::engine::direct_enabled().then_some(cfg.engine.shards);
        let engine = Engine::start(cfg).map_err(std::io::Error::other)?;
        let conns = Arc::new(AtomicUsize::new(0));
        let mut accept_threads = Vec::new();
        let (mut tcp_addr, mut unix_path) = (None, None);

        // En mode direct, chaque shard est possédé par son propre réacteur.
        let reactors = direct_shards.unwrap_or_else(reactor_count);
        #[cfg(target_os = "linux")]
        let handles = if reactors > 0 {
            let (handles, threads) =
                crate::reactor::start(&engine, &conns, reactors, token.clone());
            accept_threads.extend(threads);
            handles
        } else {
            Vec::new()
        };

        if let Some(addr) = &server_cfg.tcp_addr {
            let listener = TcpListener::bind(addr)?;
            listener.set_nonblocking(true)?;
            tcp_addr = Some(listener.local_addr()?);
            let (e, c) = (engine.clone(), conns.clone());
            if reactors > 0 {
                #[cfg(target_os = "linux")]
                {
                    let handles = handles.clone();
                    let needs_auth = token.is_some();
                    accept_threads.push(std::thread::spawn(move || {
                        let mut next = 0usize;
                        while !e.is_shutting_down() {
                            match listener.accept() {
                                Ok((s, _)) => {
                                    if c.fetch_add(1, Ordering::SeqCst)
                                        >= e.config().limits.max_connections
                                    {
                                        c.fetch_sub(1, Ordering::SeqCst);
                                        continue;
                                    }
                                    if s.set_nonblocking(true).is_err()
                                        || s.set_nodelay(true).is_err()
                                    {
                                        c.fetch_sub(1, Ordering::SeqCst);
                                        continue;
                                    }
                                    handles[next % reactors].add(s, needs_auth);
                                    next += 1;
                                }
                                Err(e) => pause_after_accept_error(&e, listener.as_raw_fd()),
                            }
                        }
                    }));
                }
            } else {
                let auth = token.clone();
                accept_threads.push(std::thread::spawn(move || {
                    accept_loop(&e, &c, auth, || {
                        let (s, _) = listener.accept().inspect_err(|err| {
                            pause_after_accept_error(err, listener.as_raw_fd())
                        })?;
                        s.set_nonblocking(false)?;
                        s.set_nodelay(true)?;
                        let w = s.try_clone()?;
                        let timeout = idle_timeout(&e);
                        s.set_read_timeout(timeout)?;
                        Ok((s, w))
                    })
                }));
            }
        }
        if let Some(path) = &server_cfg.unix_socket {
            let _ = std::fs::remove_file(path);
            let listener = UnixListener::bind(path)?;
            if let Some(mode) = server_cfg.unix_socket_mode {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))?;
            }
            listener.set_nonblocking(true)?;
            unix_path = Some(PathBuf::from(path));
            let (e, c) = (engine.clone(), conns.clone());
            #[cfg(target_os = "linux")]
            if reactors > 0 {
                let handles = handles.clone();
                accept_threads.push(std::thread::spawn(move || {
                    let mut next = 0usize;
                    while !e.is_shutting_down() {
                        match listener.accept() {
                            Ok((s, _)) => {
                                if c.fetch_add(1, Ordering::SeqCst)
                                    >= e.config().limits.max_connections
                                {
                                    c.fetch_sub(1, Ordering::SeqCst);
                                    continue;
                                }
                                if s.set_nonblocking(true).is_err() {
                                    c.fetch_sub(1, Ordering::SeqCst);
                                    continue;
                                }
                                handles[next % reactors].add(s, false);
                                next += 1;
                            }
                            Err(e) => pause_after_accept_error(&e, listener.as_raw_fd()),
                        }
                    }
                }));
                return Ok(Self {
                    engine,
                    tcp_addr,
                    unix_path,
                    accept_threads,
                });
            }
            accept_threads.push(std::thread::spawn(move || {
                accept_loop(&e, &c, None, || {
                    let (s, _) = listener
                        .accept()
                        .inspect_err(|err| pause_after_accept_error(err, listener.as_raw_fd()))?;
                    s.set_nonblocking(false)?;
                    let w = s.try_clone()?;
                    s.set_read_timeout(idle_timeout(&e))?;
                    Ok((s, w))
                })
            }));
        }
        Ok(Self {
            engine,
            tcp_addr,
            unix_path,
            accept_threads,
        })
    }

    pub fn engine(&self) -> &Arc<Engine> {
        &self.engine
    }

    pub fn tcp_addr(&self) -> Option<SocketAddr> {
        self.tcp_addr
    }

    pub fn shutdown(mut self) {
        self.engine.shutdown();
        for t in self.accept_threads.drain(..) {
            let _ = t.join();
        }
        if let Some(p) = &self.unix_path {
            let _ = std::fs::remove_file(p);
        }
    }
}

fn reactor_count() -> usize {
    #[cfg(target_os = "linux")]
    return crate::reactor::reactor_count();
    #[cfg(not(target_os = "linux"))]
    0
}

fn idle_timeout(e: &Engine) -> Option<Duration> {
    match e.config().limits.idle_timeout_ms {
        0 => None,
        ms => Some(Duration::from_millis(ms)),
    }
}

fn accept_loop<R, W>(
    engine: &Arc<Engine>,
    conns: &Arc<AtomicUsize>,
    auth: Option<Arc<Vec<u8>>>,
    mut accept: impl FnMut() -> std::io::Result<(R, W)>,
) where
    R: Read + Send + 'static,
    W: Write + Send + 'static,
{
    while !engine.is_shutting_down() {
        match accept() {
            Ok((r, w)) => {
                if conns.fetch_add(1, Ordering::SeqCst) >= engine.config().limits.max_connections {
                    conns.fetch_sub(1, Ordering::SeqCst);
                    continue; // fermeture immédiate : limite de connexions atteinte
                }
                let (e, c, a) = (engine.clone(), conns.clone(), auth.clone());
                let _ = std::thread::Builder::new()
                    .name("se-reader".into())
                    .spawn(move || {
                        serve_connection(e, r, w, a);
                        c.fetch_sub(1, Ordering::SeqCst);
                    });
            }
            Err(_) => {} // la pause est faite par la fermeture `accept`
        }
    }
}

/// Requêtes asynchrones simultanément en vol par connexion (backpressure de la lecture).
const MAX_IN_FLIGHT: usize = 256;

/// Réponse en file pour le writer : encodée par le callback (défaut) ou, avec
/// `SOLID_ENGINE_WRITER_ENCODE=1`, par le thread writer pour décharger les shards.
enum OutMsg {
    Frame(Frame),
    Resp(Response, u8, u64),
}

impl OutMsg {
    fn into_frame(self) -> Frame {
        match self {
            Self::Frame(f) => f,
            Self::Resp(r, ty, id) => r.to_frame(ty, id),
        }
    }
}

#[derive(Clone)]
struct Out(std::sync::mpsc::Sender<OutMsg>);

impl Out {
    fn send(&self, f: Frame) -> std::result::Result<(), ()> {
        self.0.send(OutMsg::Frame(f)).map_err(|_| ())
    }

    fn send_resp(&self, r: Response, ty: u8, id: u64) {
        static WRITER_ENCODE: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        let msg = if *WRITER_ENCODE
            .get_or_init(|| std::env::var("SOLID_ENGINE_WRITER_ENCODE").is_ok_and(|v| v == "1"))
        {
            OutMsg::Resp(r, ty, id)
        } else {
            OutMsg::Frame(r.to_frame(ty, id))
        };
        let _ = self.0.send(msg);
    }
}

struct InFlight {
    count: Mutex<usize>,
    cv: Condvar,
}

impl InFlight {
    fn acquire(&self) {
        let mut n = self.count.lock().unwrap();
        while *n >= MAX_IN_FLIGHT {
            n = self.cv.wait(n).unwrap();
        }
        *n += 1;
    }

    fn release(&self) {
        *self.count.lock().unwrap() -= 1;
        self.cv.notify_one();
    }
}

/// Les réponses passent par un canal et un thread d'écriture dédié par connexion : les threads
/// du moteur (shards, writer WAL) ne touchent jamais la socket, et les réponses d'une même
/// connexion peuvent sortir dans le désordre (corrélées par `request_id`).
fn serve_connection<R: Read, W: Write + Send + 'static>(
    engine: Arc<Engine>,
    reader: R,
    mut writer: W,
    auth: Option<Arc<Vec<u8>>>,
) {
    let (tx, out_rx) = std::sync::mpsc::channel::<OutMsg>();
    let out = Out(tx);
    let _ = std::thread::Builder::new()
        .name("se-writer".into())
        .spawn(move || {
            let mut buf = Vec::with_capacity(16 * 1024);
            // Toutes les réponses déjà prêtes partent dans un seul write.
            let mut gate = crate::engine::SpinGate::default();
            let mut tally =
                crate::stats::Tally::default().with_cpu_role(crate::stats::C::CpuWriterNs);
            let yield_once = std::env::var("SOLID_ENGINE_WRITER_YIELD").map_or(true, |v| v != "0");
            let linger_us = crate::engine::writer_linger_us();
            let mut linger_gate = crate::engine::SpinGate::default();
            // Trame déjà retirée du canal mais non écrite parce que le tampon était plein.
            let mut carry: Option<Frame> = None;
            loop {
                // Courte attente active avant de dormir : évite un réveil (appel système) par réponse.
                let budget = gate.budget(crate::engine::writer_spin_us());
                let spin_until =
                    std::time::Instant::now() + std::time::Duration::from_micros(budget);
                let spinning = budget > 0;
                let first = if let Some(f) = carry.take() {
                    f
                } else {
                    loop {
                        match out_rx.try_recv().map(OutMsg::into_frame) {
                            Ok(f) => {
                                if spinning {
                                    gate.record(true);
                                }
                                break f;
                            }
                            Err(std::sync::mpsc::TryRecvError::Disconnected) => return,
                            Err(_) if std::time::Instant::now() < spin_until => {
                                std::hint::spin_loop()
                            }
                            Err(_) => match {
                                tally.add(crate::stats::C::NetWriterBlockWaits, 1);
                                tally.flush();
                                out_rx.recv().map(OutMsg::into_frame)
                            } {
                                Ok(f) => {
                                    tally.add(crate::stats::C::NetWriterWakeups, 1);
                                    if spinning {
                                        gate.record(false);
                                    }
                                    break f;
                                }
                                Err(_) => return,
                            },
                        }
                    }
                };
                buf.clear();
                let mut yielded = false;
                let mut frame = first;
                // Grosse trame : écrite avec `buf` en un `writev`, sans recopier son corps.
                let mut big: Option<Frame> = None;
                loop {
                    if frame.body.len() >= BIG_FRAME_BYTES {
                        tally.add(crate::stats::C::NetWriterFrames, 1);
                        big = Some(frame);
                        break;
                    }
                    solid_engine_protocol::append_frame(&mut buf, &frame);
                    tally.add(crate::stats::C::NetWriterFrames, 1);
                    let mut r = out_rx.try_recv().map(OutMsg::into_frame);
                    if r.is_err() && !yielded && (yield_once || linger_us > 0) {
                        yielded = true;
                        if yield_once {
                            std::thread::yield_now();
                            r = out_rx.try_recv().map(OutMsg::into_frame);
                        } else {
                            // Attente bornée d'une trame de plus, seulement si la porte adaptative l'autorise.
                            let budget = linger_gate.budget(linger_us);
                            if budget > 0 {
                                let until = std::time::Instant::now()
                                    + std::time::Duration::from_micros(budget);
                                while r.is_err() && std::time::Instant::now() < until {
                                    std::hint::spin_loop();
                                    r = out_rx.try_recv().map(OutMsg::into_frame);
                                }
                                linger_gate.record(r.is_ok());
                            }
                        }
                    }
                    match r {
                        Ok(next) if buf.len() < 64 * 1024 => frame = next,
                        Ok(next) => {
                            carry = Some(next);
                            break;
                        }
                        Err(_) => break,
                    }
                }
                tally.add(crate::stats::C::NetWriteCalls, 1);
                tally.add(crate::stats::C::NetWriteBytes, buf.len() as u64);
                tally.tick();
                let t0 = if tally.is_on() {
                    crate::stats::thread_cpu_ns()
                } else {
                    0
                };
                let failed = match &big {
                    Some(f) => solid_engine_protocol::write_pending_and_frame(&mut writer, &buf, f).is_err(),
                    None => writer.write_all(&buf).is_err(),
                };
                if tally.is_on() {
                    tally.add(
                        crate::stats::C::CpuWriteSysNs,
                        crate::stats::thread_cpu_ns().saturating_sub(t0),
                    );
                }
                if failed {
                    break;
                }
            }
        });
    let mut reader =
        std::io::BufReader::with_capacity(64 * 1024, crate::stats::CountingReader::new(reader));
    let in_flight = Arc::new(InFlight {
        count: Mutex::new(0),
        cv: Condvar::new(),
    });
    let max_body = (engine.config().limits.max_payload_bytes + BODY_OVERHEAD).max(MAX_BATCH_BODY);
    let affinity = crate::engine::claim_affinity();
    let (mut last_push_shard, mut claims) = (None::<usize>, 0u32);
    let mut authed = auth.is_none();
    let mut sub: Option<Arc<Subscription>> = None;
    let _stop_sub = StopOnDrop(&mut sub as *mut _);
    loop {
        let frame = match read_frame(&mut reader, max_body) {
            Ok(f) => f,
            Err(ProtocolError::Eof) | Err(ProtocolError::Io(_)) => return,
            Err(e) => {
                // Flux désynchronisé ou invalide : on répond puis on ferme.
                let resp = Response::Error(EngineError::new(ErrorCode::BadRequest, e.to_string()));
                let _ = out.send(resp.to_frame(0, 0));
                return;
            }
        };
        let (ty, id) = (frame.msg_type, frame.request_id);
        // Soumet sans attendre : la réponse est mise en file par le callback.
        macro_rules! submit {
            ($call:expr, $map:expr) => {{
                in_flight.acquire();
                let (out, in_flight) = (out.clone(), in_flight.clone());
                $call(move |result: Result<_>| {
                    let resp = to_response(result.map($map));
                    out.send_resp(resp, ty, id);
                    in_flight.release();
                })
            }};
        }
        let decoded = Request::decode(ty, &frame.body);
        if let (false, Some(expected), Ok(req)) = (authed, &auth, &decoded) {
            let (resp, close) = auth_step(expected, &mut authed, req);
            let _ = out.send(resp.to_frame(ty, id));
            if close {
                return;
            }
            continue;
        }
        match decoded {
            Err(e) => {
                let resp = Response::Error(EngineError::new(ErrorCode::BadRequest, e.to_string()));
                let _ = out.send(resp.to_frame(ty, id));
            }
            Ok(Request::PushJob(p)) => {
                last_push_shard = Some(submit!(|d| engine.push_async(p, d), Response::Push))
            }
            Ok(Request::PushBatch(ps)) => match push_batch_too_large(ps.len()) {
                Some(e) => {
                    let _ = out.send(Response::Error(e).to_frame(ty, id));
                }
                None => {
                    in_flight.acquire();
                    let (out, in_flight) = (out.clone(), in_flight.clone());
                    engine.push_many_async(ps, move |results| {
                        out.send_resp(Response::PushBatch(results), ty, id);
                        in_flight.release();
                    });
                }
            },
            // Sans attente : le lecteur enchaîne sur la trame suivante (ex. `Credit`) pendant que les shards acquittent.
            Ok(Request::AckBatch(rs)) => {
                let n = rs.len().min(MAX_BATCH as usize);
                in_flight.acquire();
                let (out, in_flight) = (out.clone(), in_flight.clone());
                engine.ack_many_async(rs.into_iter().take(n).collect(), move |results| {
                    out.send_resp(Response::AckBatch(results), ty, id);
                    in_flight.release();
                });
            }
            Ok(Request::AckJob(r)) => submit!(|d| engine.ack_async(r, d), |_| Response::Ack),
            Ok(Request::NackJob { reservation, kind }) => {
                submit!(|d| engine.nack_async(reservation, kind, d), |_| {
                    Response::Nack
                })
            }
            Ok(Request::ExtendLease {
                reservation,
                lease_ms,
            }) => submit!(
                |d| engine.extend_lease_async(reservation, lease_ms, d),
                |lease_deadline| Response::ExtendLease { lease_deadline }
            ),
            Ok(Request::ClaimNextJob {
                channel_id,
                lease_ms,
                wait_ms,
            }) => {
                in_flight.acquire();
                let (e, out, in_flight) = (engine.clone(), out.clone(), in_flight.clone());
                let engine = engine.clone();
                // Une claim sur huit repart du balayage tournant, pour ne pas affamer les autres shards.
                claims = claims.wrapping_add(1);
                let hint = last_push_shard.filter(|_| affinity && claims % 8 != 0);
                let done = move |result: Result<Option<_>>| match result {
                    // Rien de disponible : l'attente se fait hors des threads du moteur.
                    Ok(None) if wait_ms > 0 => {
                        std::thread::spawn(move || {
                            let resp = to_response(
                                e.claim(channel_id, lease_ms, wait_ms).map(Response::Claim),
                            );
                            let _ = out.send(resp.to_frame(ty, id));
                            in_flight.release();
                        });
                    }
                    other => {
                        let _ = out.send(to_response(other.map(Response::Claim)).to_frame(ty, id));
                        in_flight.release();
                    }
                };
                match hint {
                    Some(start) => engine.try_claim_async_from(channel_id, lease_ms, start, done),
                    None => engine.try_claim_async(channel_id, lease_ms, done),
                }
            }
            Ok(Request::Subscribe {
                channel_id,
                lease_ms,
                credits,
            }) => {
                if sub.is_some() {
                    let e = EngineError::new(ErrorCode::BadRequest, "already subscribed");
                    let _ = out.send(Response::Error(e).to_frame(ty, id));
                } else {
                    let s = Arc::new(Subscription {
                        credits: Mutex::new(credits as u64),
                        cv: Condvar::new(),
                        stop: Default::default(),
                    });
                    spawn_dispatcher(
                        engine.clone(),
                        s.clone(),
                        out.clone(),
                        channel_id,
                        lease_ms,
                        id,
                    );
                    sub = Some(s);
                    let _ = out.send(Response::Subscribed.to_frame(ty, id));
                }
            }
            Ok(Request::Credit { credits }) => match &sub {
                Some(s) => s.add(credits),
                None => {
                    let e = EngineError::new(ErrorCode::BadRequest, "not subscribed");
                    let _ = out.send(Response::Error(e).to_frame(ty, id));
                }
            },
            Ok(Request::WaitForJob {
                channel_id,
                timeout_ms,
            }) if timeout_ms > 0 => {
                let (e, out) = (engine.clone(), out.clone());
                std::thread::spawn(move || {
                    let resp = to_response(
                        e.wait_for_job(channel_id, timeout_ms)
                            .map(|a| Response::Wait { available: a }),
                    );
                    let _ = out.send(resp.to_frame(ty, id));
                });
            }
            Ok(req) => {
                let _ = out.send(to_response(handle(&engine, req)).to_frame(ty, id));
            }
        }
    }
}

/// Hyperlane : crédits d'un abonnement. Le serveur ne pousse jamais plus de jobs que de crédits.
struct Subscription {
    credits: Mutex<u64>,
    cv: Condvar,
    stop: std::sync::atomic::AtomicBool,
}

impl Subscription {
    fn add(&self, n: u32) {
        let mut c = self.credits.lock().unwrap();
        *c = c.saturating_add(n as u64);
        self.cv.notify_one();
    }

    fn stop(&self) {
        self.stop.store(true, Ordering::Release);
        self.cv.notify_all();
    }
}

/// Arrête le dispatcher à la fermeture de la connexion.
struct StopOnDrop(*mut Option<Arc<Subscription>>);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        // SAFETY: pointe vers la variable locale de serve_connection, vivante pendant ce drop.
        if let Some(s) = unsafe { &*self.0 } {
            s.stop();
        }
    }
}

const DISPATCH_POLL_MS: u32 = 100;
const DISPATCH_MAX: u64 = 256;
/// Au-delà, le writer n'ajoute pas le corps de la trame à son tampon.
const BIG_FRAME_BYTES: usize = 16 * 1024;
pub(crate) const DISPATCH_MAX_BYTES: usize = 256 * 1024;

fn spawn_dispatcher(
    engine: Arc<Engine>,
    sub: Arc<Subscription>,
    out: Out,
    channel_id: u16,
    lease_ms: u32,
    id: u64,
) {
    let trace = std::env::var("SOLID_ENGINE_DISPATCH_TRACE").ok();
    let _ = std::thread::Builder::new()
        .name("se-dispatch".into())
        .spawn(move || {
            // Temps cumulés (ns) : attente de crédits, réservation, envoi au writer.
            let (mut t_credit, mut t_claim, mut t_send) = (0u64, 0u64, 0u64);
            let (mut n_disp, mut n_jobs, mut n_empty, mut n_frames) = (0u64, 0u64, 0u64, 0u64);
            let mut want_sum = 0u64;
            let t_start = std::time::Instant::now();
            macro_rules! emit {
                () => {
                    if let Some(path) = &trace {
                        if n_disp > 0 {
                            use std::io::Write;
                            let total = t_start.elapsed().as_nanos() as u64;
                            let line = format!(
                                "dispatcher {id}: total_ms={} credit_wait_ms={} claim_ms={} send_ms={} dispatches={n_disp} frames={n_frames} empty_polls={n_empty} jobs={n_jobs} jobs_per_dispatch={:.1} avg_want={:.1} {} {} {}\n",
                                total / 1_000_000,
                                t_credit / 1_000_000,
                                t_claim / 1_000_000,
                                t_send / 1_000_000,
                                n_jobs as f64 / n_disp as f64,
                                want_sum as f64 / n_disp as f64,
                                crate::engine::claim_trace::report(),
                                crate::engine::wal_trace::report(),
                                crate::engine::shard_phase::report(),
                            );
                            if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(path) {
                                let _ = f.write_all(line.as_bytes());
                            }
                        }
                    }
                };
            }
            loop {
                let t0 = std::time::Instant::now();
                let want = {
                    let mut c = sub.credits.lock().unwrap();
                    while *c == 0 && !sub.stop.load(Ordering::Acquire) {
                        c = sub.cv.wait(c).unwrap();
                    }
                    (*c).min(DISPATCH_MAX) as usize
                };
                if sub.stop.load(Ordering::Acquire) {
                    break;
                }
                let t1 = std::time::Instant::now();
                t_credit += (t1 - t0).as_nanos() as u64;
                let bodies = match engine.claim_dispatch_stream(channel_id, lease_ms, want, DISPATCH_POLL_MS) {
                    Ok(b) if b.is_empty() => {
                        n_empty += 1;
                        t_claim += t1.elapsed().as_nanos() as u64;
                        continue;
                    }
                    Ok(b) => b,
                    Err(e) => {
                        let _ = out.send(Response::Error(e).to_frame(msg::DISPATCH, id));
                        break;
                    }
                };
                let t2 = std::time::Instant::now();
                t_claim += (t2 - t1).as_nanos() as u64;
                let got: usize = bodies.iter().map(|b| b.count()).sum();
                {
                    let mut c = sub.credits.lock().unwrap();
                    *c = c.saturating_sub(got as u64);
                }
                n_disp += 1;
                n_jobs += got as u64;
                want_sum += want as u64;
                // Trames bornées en octets : un lot de gros payloads ne monopolise pas le writer.
                let mut closed = false;
                for body in bodies {
                    n_frames += 1;
                    if out.send(body.into_frame(id)).is_err() {
                        closed = true;
                        break;
                    }
                }
                t_send += t2.elapsed().as_nanos() as u64;
                if closed {
                    break;
                }
                if n_disp % 1_000 == 0 {
                    emit!();
                }
            }
            emit!();
        });
}

pub(crate) fn handle(engine: &Engine, req: Request) -> Result<Response> {
    Ok(match req {
        Request::Ping => Response::Pong,
        Request::Auth(_) => Response::Authed,
        Request::PushJob(p) => Response::Push(engine.push(p)?),
        Request::ClaimNextJob {
            channel_id,
            lease_ms,
            wait_ms,
        } => Response::Claim(engine.claim(channel_id, lease_ms, wait_ms)?),
        Request::AckJob(r) => engine.ack(r).map(|_| Response::Ack)?,
        Request::NackJob { reservation, kind } => {
            engine.nack(reservation, kind).map(|_| Response::Nack)?
        }
        Request::ExtendLease {
            reservation,
            lease_ms,
        } => Response::ExtendLease {
            lease_deadline: engine.extend_lease(reservation, lease_ms)?,
        },
        Request::GetJob { job_id } => Response::Job(engine.get(job_id)?),
        Request::CancelJob { job_id } => engine.cancel(job_id).map(|_| Response::Cancel)?,
        Request::WaitForJob {
            channel_id,
            timeout_ms,
        } => Response::Wait {
            available: engine.wait_for_job(channel_id, timeout_ms)?,
        },
        Request::GetQueueStats { channel_id } => Response::Stats(engine.stats(channel_id)?),
        Request::ClaimBatch {
            channel_id,
            lease_ms,
            max,
            wait_ms,
        } => {
            let max = max.min(MAX_BATCH) as usize;
            let mut jobs = Vec::with_capacity(max.min(64));
            if max > 0 {
                // Seul le premier job peut attendre ; les suivants ne prennent que ce qui est déjà prêt.
                match engine.claim(channel_id, lease_ms, wait_ms)? {
                    Some(first) => jobs.push(first),
                    None => return Ok(Response::ClaimBatch(jobs)),
                }
                // Les jobs déjà réservés doivent être rendus à l'appelant, même si la suite échoue.
                if let Ok(mut more) = engine.try_claim_many(channel_id, lease_ms, max - jobs.len())
                {
                    jobs.append(&mut more);
                }
            }
            Response::ClaimBatch(jobs)
        }
        Request::Subscribe { .. } | Request::Credit { .. } => {
            return Err(EngineError::new(
                ErrorCode::BadRequest,
                "subscriptions are not supported on this transport",
            ))
        }
        Request::PushBatch(ps) => {
            if let Some(e) = push_batch_too_large(ps.len()) {
                return Err(e);
            }
            Response::PushBatch(engine.push_many(ps))
        }
        Request::AckBatch(reservations) => {
            let n = reservations.len().min(MAX_BATCH as usize);
            Response::AckBatch(engine.ack_many(reservations.into_iter().take(n).collect()))
        }
    })
}

/// Plafond de jobs par requête par lot.
pub(crate) const MAX_BATCH: u32 = 1024;
/// Jobs par `PushBatch`, et taille maximale (octets) d'une trame de lot.
pub(crate) const MAX_PUSH_BATCH: usize = 256;
pub(crate) const MAX_BATCH_BODY: usize = 4 << 20;

/// Rejette un lot trop gros avant tout traitement : rien n'est publié.
pub(crate) fn push_batch_too_large(n: usize) -> Option<EngineError> {
    (n > MAX_PUSH_BATCH).then(|| {
        EngineError::new(
            ErrorCode::BadRequest,
            format!("batch exceeds {MAX_PUSH_BATCH} jobs"),
        )
    })
}

/// Jeton exigé sur TCP : SOLID_ENGINE_AUTH_TOKEN, sinon `auth_token_file`, sinon `auth_token`.
fn resolve_token(
    cfg: &solid_engine_core::config::ServerConfig,
) -> std::io::Result<Option<Arc<Vec<u8>>>> {
    let token = if let Some(t) = std::env::var_os("SOLID_ENGINE_AUTH_TOKEN") {
        t.to_string_lossy().into_owned()
    } else if let Some(path) = &cfg.auth_token_file {
        std::fs::read_to_string(path)?
            .lines()
            .next()
            .unwrap_or("")
            .trim()
            .to_owned()
    } else if let Some(t) = &cfg.auth_token {
        t.clone()
    } else {
        return Ok(None);
    };
    if token.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "auth token is empty",
        ));
    }
    Ok(Some(Arc::new(token.into_bytes())))
}

/// Comparaison sans sortie anticipée sur le contenu (la longueur peut fuiter).
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// Barrière d'authentification d'une connexion non authentifiée. Renvoie la réponse à envoyer et
/// si la connexion doit être fermée ; toute commande autre que AUTH, ou un jeton faux, la ferme.
pub(crate) fn auth_step(expected: &[u8], authed: &mut bool, req: &Request) -> (Response, bool) {
    match req {
        Request::Auth(t) if constant_time_eq(t, expected) => {
            *authed = true;
            (Response::Authed, false)
        }
        _ => (
            Response::Error(EngineError::new(
                ErrorCode::Unauthorized,
                "authentication required",
            )),
            true,
        ),
    }
}

fn to_response(r: Result<Response>) -> Response {
    r.unwrap_or_else(Response::Error)
}
