//! Micro-benchmark sans réseau : push -> claim -> ack directement sur le moteur (mode memory),
//! avec le même pipeline glissant que le banc TCP. Usage : engine_only [connexions] [fenêtre] [jobs]
use solid_engine_core::config::Config;
use solid_engine_core::*;
use solid_engine_server::Engine;
use std::sync::mpsc::channel;
use std::time::Instant;

extern "C" {
    fn getrusage(who: i32, r: *mut [i64; 18]) -> i32;
    fn clock_gettime(c: i32, t: *mut [i64; 2]) -> i32;
}
#[cfg(target_os = "macos")]
const THREAD_CPU: i32 = 16;
#[cfg(not(target_os = "macos"))]
const THREAD_CPU: i32 = 3;
fn thread_cpu_ns() -> u64 {
    let mut t = [0i64; 2];
    unsafe { clock_gettime(THREAD_CPU, &mut t) };
    t[0] as u64 * 1_000_000_000 + t[1] as u64
}
fn rusage() -> [i64; 18] {
    let mut r = [0i64; 18];
    unsafe { getrusage(0, &mut r) };
    r
}

enum Ev {
    Pushed(usize),
    Claimed(usize, Option<ClaimedJob>),
    Acked(usize),
}

fn main() {
    let mut a = std::env::args().skip(1).map(|x| x.parse::<usize>().unwrap());
    let (conns, window, jobs) = (a.next().unwrap_or(8), a.next().unwrap_or(32), a.next().unwrap_or(300_000));
    let mut cfg = Config::default();
    cfg.limits.max_jobs_per_shard = 10_000_000;
    let engine = Engine::start(cfg).unwrap();
    let per = jobs / conns;
    let ru0 = rusage();
    let st0 = solid_engine_server::stats::snapshot();
    let start = Instant::now();
    let handles: Vec<_> = (0..conns)
        .map(|c| {
            let engine = engine.clone();
            std::thread::spawn(move || {
                let cpu0 = thread_cpu_ns();
                let (tx, rx) = channel::<Ev>();
                let mut started = 0usize;
                let mut done = 0usize;
                let push = |slot: usize, started: &mut usize| {
                    *started += 1;
                    let req = PushRequest {
                        job_id: ((c as u128 + 1) << 64) | *started as u128,
                        channel_id: 1,
                        priority: 0,
                        max_attempts: 0,
                        delay_ms: 0,
                        partition_key: vec![],
                        payload: vec![b'x'; 256],
                    };
                    let tx = tx.clone();
                    engine.push_async(req, move |r| {
                        r.unwrap();
                        let _ = tx.send(Ev::Pushed(slot));
                    });
                };
                let claim = |slot: usize| {
                    let tx = tx.clone();
                    engine.try_claim_async(1, 30_000, move |r| {
                        let _ = tx.send(Ev::Claimed(slot, r.unwrap()));
                    });
                };
                for slot in 0..window.min(per) {
                    push(slot, &mut started);
                }
                while done < per {
                    match rx.recv().unwrap() {
                        Ev::Pushed(slot) => claim(slot),
                        Ev::Claimed(slot, None) => claim(slot),
                        Ev::Claimed(slot, Some(j)) => {
                            let tx = tx.clone();
                            let r = Reservation { job_id: j.job_id, reservation_id: j.reservation_id, fencing_token: j.fencing_token };
                            engine.ack_async(r, move |r| {
                                r.unwrap();
                                let _ = tx.send(Ev::Acked(slot));
                            });
                        }
                        Ev::Acked(slot) => {
                            done += 1;
                            if started < per {
                                push(slot, &mut started);
                            }
                        }
                    }
                }
                thread_cpu_ns() - cpu0
            })
        })
        .collect();
    let gen_ns: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();
    let ru1 = rusage();
    let st1 = solid_engine_server::stats::snapshot();
    let secs = start.elapsed().as_secs_f64();
    println!("engine-only memory | conns {conns} window {window} | {:.0} jobs/s", (per * conns) as f64 / secs);
    let n = (per * conns) as f64;
    let us = |a: [i64; 18], b: [i64; 18], i: usize| ((b[i] - a[i]) * 1_000_000 + (b[i + 1] - a[i + 1])) as f64;
    let (user, sys) = (us(ru0, ru1, 0) / n, us(ru0, ru1, 2) / n);
    println!("processus : user {user:.2} sys {sys:.2} total {:.2} µs/job", user + sys);
    println!("générateur (threads de connexion) : {:.2} µs/job ; reste (moteur et threads internes) : {:.2} µs/job", gen_ns as f64 / 1000.0 / n, user + sys - gen_ns as f64 / 1000.0 / n);
    println!("par job : vcsw {:.2} ivcsw {:.2} minflt {:.3}", (ru1[16] - ru0[16]) as f64 / n, (ru1[17] - ru0[17]) as f64 / n, (ru1[8] - ru0[8]) as f64 / n);
    for ((k, a), (_, b)) in st0.iter().zip(st1.iter()) {
        if b > a && (k.starts_with("shard") || k.starts_with("cpu_shard")) {
            let d = (b - a) as f64 / n;
            println!("  {k}: {:.3}", if k.ends_with("_ns") { d / 1000.0 } else { d });
        }
    }
    // Observation de la mémoire après la charge : argument 4 = durée de repos en secondes.
    let rss = || {
        let out = std::process::Command::new("ps").args(["-o", "rss=", "-p", &std::process::id().to_string()]).output().unwrap();
        String::from_utf8_lossy(&out.stdout).trim().parse::<u64>().unwrap_or(0) / 1024
    };
    let settle: u64 = std::env::args().nth(4).and_then(|v| v.parse().ok()).unwrap_or(0);
    println!("RSS t=0 s : {} MB", rss());
    for t in (15..=settle).step_by(15) {
        std::thread::sleep(std::time::Duration::from_secs(15));
        println!("RSS t={t} s : {} MB", rss());
    }
}
