//! Frontière de durabilité : en `strict`, aucun succès avant le fsync du shard concerné.
//! Fichier dédié : les points de blocage et l'injection d'erreur sont globaux au processus,
//! donc tous les scénarios tournent séquentiellement dans un seul test.
use solid_engine_core::config::{Config, Durability};
use solid_engine_core::*;
use solid_engine_protocol::{read_frame, write_frame, Request, Response};
use solid_engine_server::Server;
use solid_engine_storage::{HOLD_SYNC, INJECT_IO_ERROR, SYNC_BLOCKED};
use std::net::TcpStream;
use std::sync::atomic::Ordering::SeqCst;
use std::time::{Duration, Instant};

fn start(mode: Durability, shards: usize, tag: &str) -> (Server, TcpStream) {
    let dir = std::env::temp_dir().join(format!("solid-engine-ack-{tag}-{}-{}", std::process::id(), now_ms()));
    let mut cfg = Config::default();
    cfg.engine.shards = shards;
    cfg.server.tcp_addr = Some("127.0.0.1:0".into());
    cfg.storage.durability = mode;
    cfg.storage.sync_interval_ms = 5;
    cfg.storage.data_dir = Some(dir.to_string_lossy().into());
    let server = Server::start(cfg).unwrap();
    let s = TcpStream::connect(server.tcp_addr().unwrap()).unwrap();
    (server, s)
}

fn push(id: u128, key: &[u8]) -> Request {
    Request::PushJob(PushRequest {
        job_id: id,
        channel_id: 1,
        priority: 0,
        max_attempts: 0,
        delay_ms: 0,
        partition_key: key.to_vec(),
        payload: vec![1],
    })
}

fn send(s: &mut TcpStream, rid: u64, req: &Request) {
    write_frame(s, &req.to_frame(rid)).unwrap();
}

fn recv(s: &mut TcpStream, wait: Duration) -> Option<(u64, Response)> {
    s.set_read_timeout(Some(wait)).unwrap();
    let f = read_frame(s, 1 << 20).ok()?;
    Some((f.request_id, Response::decode(f.msg_type, &f.body).unwrap()))
}

fn wait_blocked(shard: u16) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while SYNC_BLOCKED.load(SeqCst) & (1 << shard) == 0 {
        assert!(Instant::now() < deadline, "le writer du shard {shard} n'a pas atteint le fsync");
        std::thread::sleep(Duration::from_millis(1));
    }
}

const SILENCE: Duration = Duration::from_millis(300);
const PATIENCE: Duration = Duration::from_secs(5);

fn key_for_shard(shard: usize, shards: usize) -> Vec<u8> {
    (0u32..).map(|i| i.to_le_bytes().to_vec()).find(|k| shard_for(1, k, shards) == shard).unwrap()
}

#[test]
fn success_is_never_sent_before_the_fsync_of_its_own_shard() {
    // A. fsync retenu : aucune réponse tant qu'il n'est pas terminé, toutes ensuite.
    {
        let (server, mut s) = start(Durability::Strict, 1, "a");
        HOLD_SYNC.store(1, SeqCst);
        for i in 1..=20u64 {
            send(&mut s, i, &push(i as u128, b""));
        }
        wait_blocked(0);
        assert!(recv(&mut s, SILENCE).is_none(), "réponse envoyée avant la fin du fsync");
        HOLD_SYNC.store(0, SeqCst);
        for _ in 0..20 {
            let (_, r) = recv(&mut s, PATIENCE).expect("réponse après fsync");
            assert!(matches!(r, Response::Push(_)), "{r:?}");
        }
        server.shutdown();
    }

    // B. fsync en échec : aucun succès pour le lot, et le shard reste en échec.
    {
        let (server, mut s) = start(Durability::Strict, 1, "b");
        HOLD_SYNC.store(1, SeqCst);
        for i in 1..=20u64 {
            send(&mut s, i, &push(i as u128, b""));
        }
        wait_blocked(0);
        INJECT_IO_ERROR.store(true, SeqCst);
        HOLD_SYNC.store(0, SeqCst);
        for _ in 0..20 {
            let (_, r) = recv(&mut s, PATIENCE).expect("réponse après échec");
            assert!(matches!(&r, Response::Error(e) if e.code == ErrorCode::Internal), "faux succès : {r:?}");
        }
        INJECT_IO_ERROR.store(false, SeqCst);
        send(&mut s, 99, &push(99, b""));
        let (_, r) = recv(&mut s, PATIENCE).unwrap();
        assert!(matches!(r, Response::Error(_)), "le shard doit rester en échec : {r:?}");
        server.shutdown();
    }

    // C. deux shards : le fsync de l'un ne libère pas les réponses de l'autre.
    {
        let (server, mut s) = start(Durability::Strict, 2, "c");
        let (key0, key1) = (key_for_shard(0, 2), key_for_shard(1, 2));
        HOLD_SYNC.store(1, SeqCst); // shard 0 retenu
        send(&mut s, 1, &push(1, &key0));
        wait_blocked(0);
        send(&mut s, 2, &push(2, &key1));
        let (rid, r) = recv(&mut s, PATIENCE).expect("le shard 1 n'est pas bloqué");
        assert_eq!(rid, 2);
        assert!(matches!(r, Response::Push(_)));
        assert!(recv(&mut s, SILENCE).is_none(), "réponse du shard 0 libérée par le fsync du shard 1");
        HOLD_SYNC.store(0, SeqCst);
        let (rid, r) = recv(&mut s, PATIENCE).expect("réponse du shard 0 après son fsync");
        assert_eq!(rid, 1);
        assert!(matches!(r, Response::Push(_)));
        server.shutdown();
    }

    // D. contraste : en balanced, la réponse précède le fsync (perte possible, documentée).
    {
        let (server, mut s) = start(Durability::Balanced, 1, "d");
        HOLD_SYNC.store(1, SeqCst);
        send(&mut s, 1, &push(1, b""));
        let (_, r) = recv(&mut s, PATIENCE).expect("balanced répond sans attendre le fsync");
        assert!(matches!(r, Response::Push(_)));
        HOLD_SYNC.store(0, SeqCst);
        server.shutdown();
    }
}
