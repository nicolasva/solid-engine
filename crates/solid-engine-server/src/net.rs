//! Serveur réseau : sockets Unix et TCP, connexions persistantes, requêtes en vol multiples.

use crate::engine::Engine;
use solid_engine_core::config::Config;
use solid_engine_core::*;
use solid_engine_protocol::{read_frame, write_frame, Frame, ProtocolError, Request, Response};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::os::unix::net::UnixListener;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

const ACCEPT_POLL: Duration = Duration::from_millis(20);
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
        if crate::engine::direct_enabled() && server_cfg.unix_socket.is_some() {
            return Err(std::io::Error::other("SOLID_ENGINE_REACTOR_DIRECT ne gère pas encore le socket Unix"));
        }
        let engine = Engine::start(cfg).map_err(std::io::Error::other)?;
        let conns = Arc::new(AtomicUsize::new(0));
        let mut accept_threads = Vec::new();
        let (mut tcp_addr, mut unix_path) = (None, None);

        if let Some(addr) = &server_cfg.tcp_addr {
            let listener = TcpListener::bind(addr)?;
            listener.set_nonblocking(true)?;
            tcp_addr = Some(listener.local_addr()?);
            let (e, c) = (engine.clone(), conns.clone());
            let reactors = reactor_count();
            if reactors > 0 {
                #[cfg(target_os = "linux")]
                {
                    let (mut handles, threads) = crate::reactor::start(&engine, &conns, reactors);
                    accept_threads.extend(threads);
                    accept_threads.push(std::thread::spawn(move || {
                        let mut next = 0usize;
                        while !e.is_shutting_down() {
                            match listener.accept() {
                                Ok((s, _)) => {
                                    if c.fetch_add(1, Ordering::SeqCst) >= e.config().limits.max_connections {
                                        c.fetch_sub(1, Ordering::SeqCst);
                                        continue;
                                    }
                                    if s.set_nonblocking(true).is_err() || s.set_nodelay(true).is_err() {
                                        c.fetch_sub(1, Ordering::SeqCst);
                                        continue;
                                    }
                                    handles[next % reactors].add(s);
                                    next += 1;
                                }
                                Err(_) => std::thread::sleep(ACCEPT_POLL),
                            }
                        }
                    }));
                }
            } else {
                accept_threads.push(std::thread::spawn(move || {
                    accept_loop(&e, &c, || {
                        let (s, _) = listener.accept()?;
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
            listener.set_nonblocking(true)?;
            unix_path = Some(PathBuf::from(path));
            let (e, c) = (engine.clone(), conns.clone());
            accept_threads.push(std::thread::spawn(move || {
                accept_loop(&e, &c, || {
                    let (s, _) = listener.accept()?;
                    s.set_nonblocking(false)?;
                    let w = s.try_clone()?;
                    s.set_read_timeout(idle_timeout(&e))?;
                    Ok((s, w))
                })
            }));
        }
        Ok(Self { engine, tcp_addr, unix_path, accept_threads })
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
                let (e, c) = (engine.clone(), conns.clone());
                let _ = std::thread::Builder::new().name("se-reader".into()).spawn(move || {
                    serve_connection(e, r, w);
                    c.fetch_sub(1, Ordering::SeqCst);
                });
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => std::thread::sleep(ACCEPT_POLL),
            Err(_) => std::thread::sleep(ACCEPT_POLL),
        }
    }
}

/// Requêtes asynchrones simultanément en vol par connexion (backpressure de la lecture).
const MAX_IN_FLIGHT: usize = 256;

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
fn serve_connection<R: Read, W: Write + Send + 'static>(engine: Arc<Engine>, reader: R, mut writer: W) {
    let (out, out_rx) = std::sync::mpsc::channel::<Frame>();
    let _ = std::thread::Builder::new().name("se-writer".into()).spawn(move || {
        let mut buf = Vec::with_capacity(16 * 1024);
        // Toutes les réponses déjà prêtes partent dans un seul write.
        let mut gate = crate::engine::SpinGate::default();
        let mut tally = crate::stats::Tally::default().with_cpu_role(crate::stats::C::CpuWriterNs);
        let yield_once = std::env::var_os("SOLID_ENGINE_WRITER_YIELD").is_some();
        let linger_us = crate::engine::writer_linger_us();
        let mut linger_gate = crate::engine::SpinGate::default();
        loop {
            // Courte attente active avant de dormir : évite un réveil (appel système) par réponse.
            let budget = gate.budget(crate::engine::writer_spin_us());
            let spin_until = std::time::Instant::now() + std::time::Duration::from_micros(budget);
            let spinning = budget > 0;
            let first = loop {
                match out_rx.try_recv() {
                    Ok(f) => {
                        if spinning {
                            gate.record(true);
                        }
                        break f;
                    }
                    Err(std::sync::mpsc::TryRecvError::Disconnected) => return,
                    Err(_) if std::time::Instant::now() < spin_until => std::hint::spin_loop(),
                    Err(_) => match {
                        tally.add(crate::stats::C::NetWriterBlockWaits, 1);
                        tally.flush();
                        out_rx.recv()
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
            };
            buf.clear();
            let mut yielded = false;
            let mut frame = first;
            loop {
                let _ = write_frame(&mut buf, &frame);
                tally.add(crate::stats::C::NetWriterFrames, 1);
                let mut r = out_rx.try_recv();
                if r.is_err() && !yielded && (yield_once || linger_us > 0) {
                    yielded = true;
                    if yield_once {
                        std::thread::yield_now();
                        r = out_rx.try_recv();
                    } else {
                        // Attente bornée d'une trame de plus, seulement si la porte adaptative l'autorise.
                        let budget = linger_gate.budget(linger_us);
                        if budget > 0 {
                            let until = std::time::Instant::now() + std::time::Duration::from_micros(budget);
                            while r.is_err() && std::time::Instant::now() < until {
                                std::hint::spin_loop();
                                r = out_rx.try_recv();
                            }
                            linger_gate.record(r.is_ok());
                        }
                    }
                }
                match r {
                    Ok(next) if buf.len() < 64 * 1024 => frame = next,
                    _ => break,
                }
            }
            tally.add(crate::stats::C::NetWriteCalls, 1);
            tally.add(crate::stats::C::NetWriteBytes, buf.len() as u64);
            tally.tick();
            let t0 = if tally.is_on() { crate::stats::thread_cpu_ns() } else { 0 };
            let failed = writer.write_all(&buf).is_err();
            if tally.is_on() {
                tally.add(crate::stats::C::CpuWriteSysNs, crate::stats::thread_cpu_ns().saturating_sub(t0));
            }
            if failed {
                break;
            }
        }
    });
    let mut reader = std::io::BufReader::with_capacity(64 * 1024, crate::stats::CountingReader::new(reader));
    let in_flight = Arc::new(InFlight { count: Mutex::new(0), cv: Condvar::new() });
    let max_body = engine.config().limits.max_payload_bytes + BODY_OVERHEAD;
    let affinity = crate::engine::claim_affinity();
    let (mut last_push_shard, mut claims) = (None::<usize>, 0u32);
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
                    let _ = out.send(resp.to_frame(ty, id));
                    in_flight.release();
                })
            }};
        }
        match Request::decode(ty, &frame.body) {
            Err(e) => {
                let resp = Response::Error(EngineError::new(ErrorCode::BadRequest, e.to_string()));
                let _ = out.send(resp.to_frame(ty, id));
            }
            Ok(Request::PushJob(p)) => last_push_shard = Some(submit!(|d| engine.push_async(p, d), Response::Push)),
            Ok(Request::AckJob(r)) => submit!(|d| engine.ack_async(r, d), |_| Response::Ack),
            Ok(Request::NackJob { reservation, kind }) => {
                submit!(|d| engine.nack_async(reservation, kind, d), |_| Response::Nack)
            }
            Ok(Request::ExtendLease { reservation, lease_ms }) => submit!(
                |d| engine.extend_lease_async(reservation, lease_ms, d),
                |lease_deadline| Response::ExtendLease { lease_deadline }
            ),
            Ok(Request::ClaimNextJob { channel_id, lease_ms, wait_ms }) => {
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
                            let resp = to_response(e.claim(channel_id, lease_ms, wait_ms).map(Response::Claim));
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
            Ok(Request::WaitForJob { channel_id, timeout_ms }) if timeout_ms > 0 => {
                let (e, out) = (engine.clone(), out.clone());
                std::thread::spawn(move || {
                    let resp = to_response(e.wait_for_job(channel_id, timeout_ms).map(|a| Response::Wait { available: a }));
                    let _ = out.send(resp.to_frame(ty, id));
                });
            }
            Ok(req) => {
                let _ = out.send(to_response(handle(&engine, req)).to_frame(ty, id));
            }
        }
    }
}

pub(crate) fn handle(engine: &Engine, req: Request) -> Result<Response> {
    Ok(match req {
        Request::Ping => Response::Pong,
        Request::PushJob(p) => Response::Push(engine.push(p)?),
        Request::ClaimNextJob { channel_id, lease_ms, wait_ms } => {
            Response::Claim(engine.claim(channel_id, lease_ms, wait_ms)?)
        }
        Request::AckJob(r) => engine.ack(r).map(|_| Response::Ack)?,
        Request::NackJob { reservation, kind } => engine.nack(reservation, kind).map(|_| Response::Nack)?,
        Request::ExtendLease { reservation, lease_ms } => Response::ExtendLease {
            lease_deadline: engine.extend_lease(reservation, lease_ms)?,
        },
        Request::GetJob { job_id } => Response::Job(engine.get(job_id)?),
        Request::CancelJob { job_id } => engine.cancel(job_id).map(|_| Response::Cancel)?,
        Request::WaitForJob { channel_id, timeout_ms } => {
            Response::Wait { available: engine.wait_for_job(channel_id, timeout_ms)? }
        }
        Request::GetQueueStats { channel_id } => Response::Stats(engine.stats(channel_id)?),
    })
}

fn to_response(r: Result<Response>) -> Response {
    r.unwrap_or_else(Response::Error)
}
