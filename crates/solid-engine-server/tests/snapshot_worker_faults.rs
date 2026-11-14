//! camp35-E1 : fautes aux frontières de publication du worker de snapshot (mode balanced).
//! Contrôle : la récupération réussit et les jobs présents forment un préfixe contigu des Push émis
//! (1 shard, 1 client). En balanced, un SIGKILL peut perdre des Push déjà acquittés ; aucune corruption
//! ni rupture de séquence ne doit apparaître.
use solid_engine_core::config::{Config, Durability};
use solid_engine_core::*;
use solid_engine_protocol::client::Client;
use solid_engine_protocol::{Request, Response};
use solid_engine_server::Server;
use solid_engine_storage::SNAPSHOT_FAIL_AT;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::atomic::Ordering;
use std::sync::Mutex;
use std::time::{Duration, Instant};

// SNAPSHOT_FAIL_AT est global au processus : les tests de ce fichier ne doivent pas s'exécuter en parallèle.
static SERIAL: Mutex<()> = Mutex::new(());

fn push_req(id: u128) -> Request {
    Request::PushJob(PushRequest { job_id: id, channel_id: 1, priority: 0, max_attempts: 0, delay_ms: 0, partition_key: vec![], payload: vec![9; 32] })
}

fn config(dir: &Path) -> Config {
    let mut cfg = Config::default();
    cfg.server.tcp_addr = Some("127.0.0.1:0".into());
    cfg.engine.shards = 1;
    cfg.storage.durability = Durability::Balanced;
    cfg.storage.data_dir = Some(dir.join("data").to_string_lossy().into());
    cfg.storage.snapshot_every_records = 20;
    cfg
}

/// Renvoie le nombre de jobs présents ; échoue si l'ensemble n'est pas le préfixe 1..=k de 1..=max.
fn check_recovery(dir: &Path, max: u128, tag: &str) -> u128 {
    let server = Server::start(config(dir)).unwrap_or_else(|e| panic!("{tag}: la récupération échoue : {e:?}"));
    let mut c = Client::connect_tcp(server.tcp_addr().unwrap()).unwrap();
    let mut present = 0u128;
    let mut gap_after = None;
    for id in 1..=max {
        let here = matches!(c.call(&Request::GetJob { job_id: id }).unwrap(), Response::Job(_));
        if here {
            assert!(gap_after.is_none(), "{tag}: job {id} présent après le trou {gap_after:?}");
            present += 1;
        } else if gap_after.is_none() {
            gap_after = Some(id);
        }
    }
    assert!(matches!(c.call(&push_req(max + 1_000_000)).unwrap(), Response::Push(_)), "{tag}: push après récupération");
    server.shutdown();
    present
}

fn fresh_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("se-wf-{tag}-{}-{}", std::process::id(), now_ms()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn worker_crash_at_each_boundary_recovers_consistently() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    for boundary in 1..=6u8 {
        let dir = fresh_dir(&format!("crash{boundary}"));
        let sock = dir.join("e.sock");
        let toml = dir.join("engine.toml");
        std::fs::write(
            &toml,
            format!(
                "[engine]\nshards = 1\n\n[server]\nunix_socket = \"{}\"\n\n[storage]\ndurability = \"balanced\"\ndata_dir = \"{}\"\nsnapshot_every_records = 20\n",
                sock.display(),
                dir.join("data").display()
            ),
        )
        .unwrap();
        let mut child = Command::new(env!("CARGO_BIN_EXE_solid-engine-server"))
            .arg("--config")
            .arg(&toml)
            .env("SOLID_ENGINE_SNAPSHOT_WORKER", "1")
            .env("SOLID_ENGINE_TEST_SNAPSHOT_CRASH_AT", boundary.to_string())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        while Client::connect_unix(&sock).is_err() {
            assert!(Instant::now() < deadline, "serveur non démarré");
            std::thread::sleep(Duration::from_millis(20));
        }
        let mut c = Client::connect_unix(&sock).unwrap();
        let mut sent = 0u128;
        while sent < 2000 {
            if c.call(&push_req(sent + 1)).is_err() {
                break;
            }
            sent += 1;
        }
        let died = child.try_wait().unwrap().is_some() || {
            std::thread::sleep(Duration::from_millis(500));
            child.try_wait().unwrap().is_some()
        };
        assert!(died, "frontière {boundary} : le processus n'a pas été arrêté");
        let _ = child.wait();
        let present = check_recovery(&dir, sent + 50, &format!("crash@{boundary}"));
        println!("crash@{boundary}: envoyés={sent} présents={present}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[test]
fn worker_injected_errors_fail_shard_and_recover() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    std::env::set_var("SOLID_ENGINE_SNAPSHOT_WORKER", "1");
    for boundary in 1..=6u8 {
        let dir = fresh_dir(&format!("err{boundary}"));
        let mut sent = 0u128;
        {
            let server = Server::start(config(&dir)).unwrap();
            let mut c = Client::connect_tcp(server.tcp_addr().unwrap()).unwrap();
            for i in 1..=30u128 {
                assert!(matches!(c.call(&push_req(i)).unwrap(), Response::Push(_)));
                sent = i;
            }
            std::thread::sleep(Duration::from_millis(200));
            SNAPSHOT_FAIL_AT.store(boundary, Ordering::SeqCst);
            for i in 31..=400u128 {
                match c.call(&push_req(i)) {
                    Ok(Response::Push(_)) => sent = i,
                    _ => break,
                }
            }
            let mut failed = sent < 400;
            for i in 401..=600u128 {
                std::thread::sleep(Duration::from_millis(10));
                match c.call(&push_req(i)) {
                    Ok(Response::Push(_)) => sent = i,
                    _ => {
                        failed = true;
                        break;
                    }
                }
            }
            println!("err@{boundary}: échec du shard observé = {failed}");
            SNAPSHOT_FAIL_AT.store(0, Ordering::SeqCst);
            server.shutdown();
        }
        let present = check_recovery(&dir, sent + 50, &format!("err@{boundary}"));
        println!("err@{boundary}: acquittés={sent} présents={present}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[test]
fn worker_rapid_restart_cycles_release_lock_and_keep_prefix() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    if std::env::var("SOLID_ENGINE_SNAPSHOT_WORKER").is_err() {
        std::env::set_var("SOLID_ENGINE_SNAPSHOT_WORKER", "1");
    }
    let dir = fresh_dir("cycle");
    let mut total = 0u128;
    for round in 0..150u32 {
        let server = Server::start(config(&dir)).unwrap_or_else(|e| panic!("tour {round} : {e:?}"));
        let mut c = Client::connect_tcp(server.tcp_addr().unwrap()).unwrap();
        for _ in 0..45 {
            total += 1;
            let r = c.call(&push_req(total));
            assert!(matches!(r, Ok(Response::Push(_))), "tour {round}, push {total} : {r:?}");
        }
        server.shutdown();
    }
    let present = check_recovery(&dir, total, "cycle");
    assert_eq!(present, total, "arrêts propres : tous les jobs doivent être présents");
    let _ = std::fs::remove_dir_all(&dir);
}

/// Séquence déterministe : le snapshot A (lent) est en vol, B est mis en attente à 40 records, 5 Push
/// arrivent encore, puis B est publié. Après un arrêt propre, les 45 jobs doivent être présents.
#[test]
fn worker_pending_snapshot_keeps_records_appended_after_its_state() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    if std::env::var("SOLID_ENGINE_SNAPSHOT_WORKER").is_err() {
        std::env::set_var("SOLID_ENGINE_SNAPSHOT_WORKER", "1");
    }
    let dir = fresh_dir("pending");
    solid_engine_storage::SNAPSHOT_DELAY_MS.store(150, Ordering::SeqCst);
    let mut total = 0u128;
    for round in 0..3u32 {
        let server = Server::start(config(&dir)).unwrap_or_else(|e| panic!("tour {round} : {e:?}"));
        let mut c = Client::connect_tcp(server.tcp_addr().unwrap()).unwrap();
        for _ in 0..45 {
            total += 1;
            let r = c.call(&push_req(total));
            assert!(matches!(r, Ok(Response::Push(_))), "tour {round}, push {total} : {r:?}");
        }
        std::thread::sleep(Duration::from_millis(700));
        server.shutdown();
    }
    solid_engine_storage::SNAPSHOT_DELAY_MS.store(0, Ordering::SeqCst);
    assert_eq!(check_recovery(&dir, total, "pending"), total);
    let _ = std::fs::remove_dir_all(&dir);
}
