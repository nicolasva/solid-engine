//! Multiplexage : réponses hors ordre et déconnexions avec 256 requêtes en vol, dans les 3 modes.
use solid_engine_core::config::{Config, Durability};
use solid_engine_core::*;
use solid_engine_protocol::client::Client;
use solid_engine_protocol::{read_frame, write_frame, Request, Response};
use solid_engine_server::Server;
use std::collections::HashSet;
use std::net::{Shutdown, TcpStream};
use std::time::Duration;

const MODES: [Durability; 3] = [Durability::Memory, Durability::Balanced, Durability::Strict];

fn start(mode: Durability, max_connections: usize) -> (Server, String) {
    let dir = std::env::temp_dir().join(format!("solid-engine-pipe-{}-{}", std::process::id(), now_ms()));
    let mut cfg = Config::default();
    cfg.server.tcp_addr = Some("127.0.0.1:0".into());
    cfg.limits.max_connections = max_connections;
    cfg.storage.durability = mode;
    if mode != Durability::Memory {
        cfg.storage.data_dir = Some(dir.to_string_lossy().into());
    }
    if std::env::var_os("SOLID_ENGINE_REACTOR_DIRECT").is_some() {
        cfg.engine.shards = 1;
    }
    let server = Server::start(cfg).unwrap();
    let addr = server.tcp_addr().unwrap().to_string();
    (server, addr)
}

fn push(id: u128, channel_id: u16) -> Request {
    Request::PushJob(PushRequest {
        job_id: id,
        channel_id,
        priority: 0,
        max_attempts: 0,
        delay_ms: 0,
        partition_key: vec![],
        payload: vec![1],
    })
}

fn connect(addr: &str) -> TcpStream {
    let s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    s
}

fn works(addr: &str) {
    let mut c = Client::connect_tcp(addr).unwrap();
    assert!(matches!(c.call(&Request::Ping).unwrap(), Response::Pong { .. }));
}

#[test]
fn responses_arrive_out_of_order_and_match_their_request_id() {
    for mode in MODES {
        let (server, addr) = start(mode, 1024);
        let mut s = connect(&addr);
        // Un claim en attente sur une queue vide répond après 400 ms, après toutes les écritures.
        const SLOW: u64 = 1000;
        let slow = Request::ClaimNextJob { channel_id: 9, lease_ms: 1000, wait_ms: 400 };
        write_frame(&mut s, &slow.to_frame(SLOW)).unwrap();
        for i in 1..=255u64 {
            write_frame(&mut s, &push(i as u128, 1).to_frame(i)).unwrap();
        }
        let mut order = Vec::new();
        for _ in 0..256 {
            let f = read_frame(&mut s, 1 << 20).unwrap();
            let resp = Response::decode(f.msg_type & 0x7F, &f.body).unwrap();
            match f.request_id {
                SLOW => assert!(matches!(resp, Response::Claim(None)), "{mode:?}: {resp:?}"),
                _ => assert!(matches!(resp, Response::Push(_)), "{mode:?}: {resp:?}"),
            }
            order.push(f.request_id);
        }
        let unique: HashSet<_> = order.iter().copied().collect();
        assert_eq!(unique.len(), 256, "{mode:?}: réponse perdue ou dupliquée");
        assert!(unique.contains(&SLOW) && (1..=255).all(|i| unique.contains(&i)));
        assert_eq!(*order.last().unwrap(), SLOW, "{mode:?}: la réponse lente doit arriver en dernier");
        assert_ne!(order[0], 1000, "{mode:?}");
        server.shutdown();
    }
}

#[test]
fn abrupt_disconnects_with_256_in_flight_leak_nothing() {
    for mode in MODES {
        // Limite basse : une connexion fuitée finirait par refuser les suivantes.
        let (server, addr) = start(mode, 4);
        for round in 0..24u64 {
            let mut s = connect(&addr);
            for i in 0..256u64 {
                write_frame(&mut s, &push(((round + 1) as u128) << 32 | i as u128, 1).to_frame(i)).unwrap();
            }
            // Coupure à des étapes différentes : tout de suite, ou après quelques réponses.
            for _ in 0..(round % 4) * 20 {
                read_frame(&mut s, 1 << 20).unwrap();
            }
            s.shutdown(Shutdown::Both).unwrap();
            drop(s);
            works(&addr); // les autres connexions continuent de fonctionner
        }
        // Les 24 connexions coupées ont été libérées : la limite de 4 n'est pas atteinte.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let mut held: Vec<TcpStream> = Vec::new();
        while held.len() < 4 {
            assert!(std::time::Instant::now() < deadline, "{mode:?}: connexions non libérées");
            let mut s = connect(&addr);
            write_frame(&mut s, &Request::Ping.to_frame(1)).unwrap();
            match read_frame(&mut s, 1 << 20) {
                Ok(_) => held.push(s),
                Err(_) => std::thread::sleep(Duration::from_millis(50)),
            }
        }
        // Les requêtes déjà acceptées peuvent avoir été exécutées : le moteur reste cohérent.
        let mut c = Client::connect_tcp(&addr).unwrap_or_else(|_| panic!("{mode:?}"));
        drop(held);
        std::thread::sleep(Duration::from_millis(100));
        let Response::Stats(st) = c.call(&Request::GetQueueStats { channel_id: Some(1) }).unwrap() else { panic!() };
        assert!(st.ready <= 24 * 256, "{mode:?}: {st:?}");
        server.shutdown();
    }
}
