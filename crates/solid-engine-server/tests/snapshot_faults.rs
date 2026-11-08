//! camp34 phase 1B : fautes autour des snapshots à travers le serveur réel (TCP, shards, writer partagé).
//! Fichier dédié : les drapeaux d'injection sont globaux au processus. En Strict, tout Push acquitté
//! doit être retrouvé après redémarrage ; un Push refusé peut exister ou non.
use solid_engine_core::config::{Config, Durability};
use solid_engine_core::*;
use solid_engine_protocol::client::Client;
use solid_engine_protocol::{Request, Response};
use solid_engine_server::Server;
use solid_engine_storage::{INJECT_IO_ERROR, SNAPSHOT_DELAY_MS};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

fn config(dir: &Path) -> Config {
    let mut cfg = Config::default();
    cfg.server.tcp_addr = Some("127.0.0.1:0".into());
    cfg.engine.shards = 2;
    cfg.storage.durability = Durability::Strict;
    cfg.storage.data_dir = Some(dir.join("data").to_string_lossy().into());
    cfg.storage.snapshot_every_records = 20;
    cfg
}

fn push(c: &mut Client<std::net::TcpStream>, id: u128) -> bool {
    let p = PushRequest { job_id: id, channel_id: 1, priority: 0, max_attempts: 0, delay_ms: 0, partition_key: vec![], payload: vec![9; 32] };
    matches!(c.call(&Request::PushJob(p)), Ok(Response::Push(_)))
}

fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
    for e in std::fs::read_dir(dir).unwrap().flatten() {
        let p = e.path();
        if p.is_dir() {
            walk(&p, out);
        } else {
            out.push(p);
        }
    }
}

fn files(dir: &Path) -> Vec<PathBuf> {
    let mut v = vec![];
    walk(&dir.join("data"), &mut v);
    v
}

fn assert_all_present(dir: &Path, acked: &[u128], tag: &str) {
    let server = Server::start(config(dir)).unwrap_or_else(|e| panic!("{tag}: la récupération échoue : {e:?}"));
    let mut c = Client::connect_tcp(server.tcp_addr().unwrap()).unwrap();
    for id in acked {
        assert!(matches!(c.call(&Request::GetJob { job_id: *id }).unwrap(), Response::Job(_)), "{tag}: job acquitté {id} perdu");
    }
    server.shutdown();
}

#[test]
fn snapshot_faults_keep_acked_jobs() {
    let dir = std::env::temp_dir().join(format!("solid-engine-snapfault-{}-{}", std::process::id(), now_ms()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mut acked: Vec<u128> = vec![];
    let mut next = 1u128;

    // 1. snapshots nominaux fréquents
    {
        let server = Server::start(config(&dir)).unwrap();
        let mut c = Client::connect_tcp(server.tcp_addr().unwrap()).unwrap();
        for _ in 0..300 {
            assert!(push(&mut c, next), "push nominal {next}");
            acked.push(next);
            next += 1;
        }
        server.shutdown();
    }
    let snaps = files(&dir).iter().filter(|p| p.extension().is_some_and(|e| e == "snap")).count();
    assert!(snaps >= 1, "aucun snapshot publié");
    assert_all_present(&dir, &acked, "nominal");

    // 2. erreur d'I/O (écriture, fsync) pendant la phase des snapshots, avec snapshots lents
    {
        let server = Server::start(config(&dir)).unwrap();
        let mut c = Client::connect_tcp(server.tcp_addr().unwrap()).unwrap();
        SNAPSHOT_DELAY_MS.store(30, Ordering::SeqCst);
        let mut refused = 0;
        for i in 0..200 {
            if i == 60 {
                INJECT_IO_ERROR.store(true, Ordering::SeqCst);
            }
            if push(&mut c, next) {
                acked.push(next);
            } else {
                refused += 1;
            }
            next += 1;
        }
        INJECT_IO_ERROR.store(false, Ordering::SeqCst);
        SNAPSHOT_DELAY_MS.store(0, Ordering::SeqCst);
        assert!(refused > 0, "l'erreur injectée n'a jamais été propagée au client");
        server.shutdown();
    }
    assert_all_present(&dir, &acked, "après erreur d'I/O");

    // 3. arrêt pendant un snapshot lent
    {
        let server = Server::start(config(&dir)).unwrap();
        let mut c = Client::connect_tcp(server.tcp_addr().unwrap()).unwrap();
        SNAPSHOT_DELAY_MS.store(200, Ordering::SeqCst);
        for _ in 0..60 {
            if push(&mut c, next) {
                acked.push(next);
            }
            next += 1;
        }
        server.shutdown();
        SNAPSHOT_DELAY_MS.store(0, Ordering::SeqCst);
    }
    assert_all_present(&dir, &acked, "arrêt pendant snapshot lent");

    // 4. snapshot interrompu : fichier temporaire tronqué et snapshot final tronqué non publié
    let mut cut = 0;
    for p in files(&dir) {
        let name = p.file_name().unwrap().to_string_lossy().to_string();
        if name.starts_with("snap-") && name.ends_with(".snap") {
            let data = std::fs::read(&p).unwrap();
            let tmp = p.with_file_name(format!("snap-{:020}.snap.tmp", u64::MAX / 2 + cut));
            std::fs::write(tmp, &data[..data.len() / 2]).unwrap();
            cut += 1;
        }
    }
    assert!(cut > 0);
    assert_all_present(&dir, &acked, "snapshot interrompu (.tmp tronqué)");
    let _ = std::fs::remove_dir_all(&dir);
}
