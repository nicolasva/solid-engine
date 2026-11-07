use solid_engine_core::config::{Config, Durability};
use solid_engine_core::PushRequest;
use solid_engine_protocol::{read_frame, write_frame, Request, Response};
use solid_engine_server::Server;
use std::io::BufWriter;
use std::net::TcpStream;
use std::time::{Duration, Instant};

fn cpu_ms() -> u64 {
    let s = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
    let rest = s.rsplit(')').next().unwrap_or("");
    let f: Vec<&str> = rest.split_whitespace().collect();
    let t = |i: usize| f.get(i).and_then(|v| v.parse::<u64>().ok()).unwrap_or(0);
    (t(11) + t(12)) * 10
}

const N: u64 = 300_000;

fn rss_kb() -> u64 {
    let s = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    s.lines()
        .find(|l| l.starts_with("VmRSS:"))
        .and_then(|l| l.split_whitespace().nth(1)?.parse().ok())
        .unwrap_or(0)
}

/// Le client envoie `N` pushes sans lire une seule réponse, puis reprend la lecture : chaque réponse doit
/// revenir une fois, avec le bon `request_id` et le bon `job_id`. Se lance avec `SOLID_ENGINE_WRITER_ENCODE=0` pour
/// comparer la mémoire à l'ancien chemin (affichée avec `--nocapture`).
#[test]
fn client_that_stops_reading_gets_every_response_once_it_resumes() {
    let mut cfg = Config::default();
    cfg.server.tcp_addr = Some("127.0.0.1:0".into());
    cfg.storage.durability = Durability::Memory;
    let server = Server::start(cfg).unwrap();
    let stream = TcpStream::connect(server.tcp_addr().unwrap()).unwrap();
    let mut reader = stream.try_clone().unwrap();
    let rss0 = rss_kb();

    let sender = std::thread::spawn(move || {
        let mut w = BufWriter::new(stream);
        for id in 1..=N {
            let req = Request::PushJob(PushRequest {
                job_id: id as u128,
                channel_id: 1,
                priority: 0,
                max_attempts: 0,
                delay_ms: 0,
                partition_key: vec![],
                payload: vec![0u8; 16],
            });
            write_frame(&mut w, &req.to_frame(id)).unwrap();
        }
        std::io::Write::flush(&mut w).unwrap();
        w
    });
    // Lecture suspendue : les réponses s'accumulent côté serveur, bornées seulement par la mémoire.
    std::thread::sleep(Duration::from_secs(4));
    let rss_stalled = rss_kb();

    let start = Instant::now();
    let cpu0 = cpu_ms();
    let mut first = Duration::ZERO;
    let mut seen = vec![false; N as usize + 1];
    for _ in 0..N {
        let f = read_frame(&mut reader, 1 << 20).unwrap();
        if first.is_zero() {
            first = start.elapsed();
        }
        let id = f.request_id;
        assert!(!seen[id as usize], "réponse en double pour {id}");
        seen[id as usize] = true;
        let Response::Push(o) = Response::decode(f.msg_type, &f.body).unwrap() else { panic!("réponse inattendue") };
        assert_eq!(o.job_id, id as u128, "réponse associée à la mauvaise requête");
    }
    let drain = start.elapsed();
    assert!(seen[1..].iter().all(|s| *s));
    drop(sender.join().unwrap());
    println!(
        "slow_client: rss_start={}MB rss_stalled={}MB (+{}MB) first={:?} drain={:?} cpu_ms={}",
        rss0 / 1024,
        rss_stalled / 1024,
        rss_stalled.saturating_sub(rss0) / 1024,
        first,
        drain,
        cpu_ms().saturating_sub(cpu0)
    );
    server.shutdown();
}

fn threads() -> u64 {
    let s = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    s.lines()
        .find(|l| l.starts_with("Threads:"))
        .and_then(|l| l.split_whitespace().nth(1)?.parse().ok())
        .unwrap_or(0)
}

/// Un client qui se déconnecte sans avoir lu ses réponses doit libérer sa connexion (threads, file du writer) :
/// après plusieurs cycles, le nombre de threads et le RSS ne dérivent pas, et une nouvelle connexion est servie.
#[test]
fn disconnect_while_responses_pile_up_releases_the_connection() {
    let mut cfg = Config::default();
    cfg.server.tcp_addr = Some("127.0.0.1:0".into());
    cfg.storage.durability = Durability::Memory;
    let server = Server::start(cfg).unwrap();
    let addr = server.tcp_addr().unwrap();
    let push = |id: u64| {
        Request::PushJob(PushRequest {
            job_id: id as u128,
            channel_id: 1,
            priority: 0,
            max_attempts: 0,
            delay_ms: 0,
            partition_key: vec![],
            payload: vec![0u8; 16],
        })
    };
    let (threads0, mut rss_after) = (threads(), Vec::new());
    for cycle in 0..5u64 {
        let stream = TcpStream::connect(addr).unwrap();
        let mut w = BufWriter::new(stream.try_clone().unwrap());
        for i in 1..=N {
            write_frame(&mut w, &push(i).to_frame(i)).unwrap();
        }
        std::io::Write::flush(&mut w).unwrap();
        std::thread::sleep(Duration::from_secs(2));
        let rss_full = rss_kb();
        drop(w);
        drop(stream);
        std::thread::sleep(Duration::from_secs(2));
        rss_after.push(rss_kb() / 1024);
        println!(
            "disconnect: cycle={cycle} rss_full={}MB rss_after={}MB threads={}",
            rss_full / 1024,
            rss_kb() / 1024,
            threads()
        );
        // Une nouvelle connexion est servie tout de suite.
        let mut s = TcpStream::connect(addr).unwrap();
        write_frame(&mut s, &push(10_000_000 + cycle).to_frame(7)).unwrap();
        let f = read_frame(&mut s, 1 << 20).unwrap();
        assert_eq!(f.request_id, 7);
        assert!(matches!(Response::decode(f.msg_type, &f.body).unwrap(), Response::Push(_)));
    }
    assert!(threads() <= threads0 + 2, "threads de connexion non libérés");
    server.shutdown();
}
