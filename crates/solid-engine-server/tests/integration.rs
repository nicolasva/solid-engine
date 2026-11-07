use solid_engine_core::config::Config;
use solid_engine_core::*;
use solid_engine_protocol::client::Client;
use solid_engine_protocol::{Request, Response};
use solid_engine_server::Server;
use std::io::{Read, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn start(tweak: impl FnOnce(&mut Config)) -> (Server, String) {
    let mut cfg = Config::default();
    cfg.server.tcp_addr = Some("127.0.0.1:0".into());
    tweak(&mut cfg);
    let server = Server::start(cfg).unwrap();
    let addr = server.tcp_addr().unwrap().to_string();
    (server, addr)
}

fn push(job_id: u128, delay_ms: u64) -> Request {
    Request::PushJob(PushRequest {
        job_id,
        channel_id: 1,
        priority: 0,
        max_attempts: 0,
        delay_ms,
        partition_key: vec![],
        payload: b"hello".to_vec(),
    })
}

fn claim(c: &mut Client<impl Read + Write>, wait_ms: u32) -> Option<ClaimedJob> {
    match c
        .call(&Request::ClaimNextJob {
            channel_id: 1,
            lease_ms: 0,
            wait_ms,
        })
        .unwrap()
    {
        Response::Claim(j) => j,
        other => panic!("unexpected {other:?}"),
    }
}

fn reservation(j: &ClaimedJob) -> Reservation {
    Reservation {
        job_id: j.job_id,
        reservation_id: j.reservation_id,
        fencing_token: j.fencing_token,
    }
}

#[test]
fn push_claim_ack_over_tcp_and_unix() {
    let sock = std::env::temp_dir().join(format!("solid-engine-test-{}.sock", std::process::id()));
    let (server, addr) = start(|c| c.server.unix_socket = Some(sock.to_string_lossy().into()));
    let mut tcp = Client::connect_tcp(&addr).unwrap();
    let mut unix = Client::connect_unix(&sock).unwrap();

    assert_eq!(tcp.call(&Request::Ping).unwrap(), Response::Pong);
    let Response::Push(out) = tcp.call(&push(0, 0)).unwrap() else {
        panic!()
    };
    assert_ne!(out.job_id, 0);

    let job = claim(&mut unix, 0).expect("job available through the other transport");
    assert_eq!(job.payload, b"hello");
    assert_eq!(job.fencing_token, 1);
    assert_eq!(
        unix.call(&Request::AckJob(reservation(&job))).unwrap(),
        Response::Ack
    );

    // Un ACK rejoué est rejeté.
    let Response::Error(e) = unix.call(&Request::AckJob(reservation(&job))).unwrap() else {
        panic!()
    };
    assert_eq!(e.code, ErrorCode::StaleReservation);

    let Response::Job(view) = tcp.call(&Request::GetJob { job_id: out.job_id }).unwrap() else {
        panic!()
    };
    assert_eq!(view.state, JobState::Completed);
    server.shutdown();
}

#[test]
fn idempotent_push_and_cancel() {
    let (server, addr) = start(|_| {});
    let mut c = Client::connect_tcp(&addr).unwrap();
    let Response::Push(a) = c.call(&push(99, 0)).unwrap() else {
        panic!()
    };
    let Response::Push(b) = c.call(&push(99, 0)).unwrap() else {
        panic!()
    };
    assert!(!a.duplicate && b.duplicate);
    assert_eq!(
        c.call(&Request::CancelJob { job_id: 99 }).unwrap(),
        Response::Cancel
    );
    assert!(claim(&mut c, 0).is_none());
    let Response::Error(e) = c.call(&Request::GetJob { job_id: 12345 }).unwrap() else {
        panic!()
    };
    assert_eq!(e.code, ErrorCode::NotFound);
    server.shutdown();
}

#[test]
fn waiting_claim_is_woken_by_push_without_polling() {
    let (server, addr) = start(|_| {});
    let addr2 = addr.clone();
    let waiter = std::thread::spawn(move || {
        let mut c = Client::connect_tcp(&addr2).unwrap();
        let start = Instant::now();
        (claim(&mut c, 5_000), start.elapsed())
    });
    std::thread::sleep(Duration::from_millis(200));
    let mut p = Client::connect_tcp(&addr).unwrap();
    p.call(&push(5, 0)).unwrap();
    let (job, waited) = waiter.join().unwrap();
    assert_eq!(job.unwrap().job_id, 5);
    assert!(waited < Duration::from_secs(2), "woke after {waited:?}");
    server.shutdown();
}

#[test]
fn waiting_claim_times_out_without_blocking_others() {
    let (server, addr) = start(|_| {});
    let addr2 = addr.clone();
    let waiter = std::thread::spawn(move || {
        let mut c = Client::connect_tcp(&addr2).unwrap();
        let start = Instant::now();
        (claim(&mut c, 400), start.elapsed())
    });
    std::thread::sleep(Duration::from_millis(100));
    let mut p = Client::connect_tcp(&addr).unwrap();
    let t = Instant::now();
    assert_eq!(p.call(&Request::Ping).unwrap(), Response::Pong);
    assert!(t.elapsed() < Duration::from_millis(100));
    let (job, waited) = waiter.join().unwrap();
    assert!(job.is_none());
    assert!(
        waited >= Duration::from_millis(380) && waited < Duration::from_secs(2),
        "{waited:?}"
    );
    server.shutdown();
}

#[test]
fn job_taken_by_a_disconnected_waiter_is_redelivered() {
    let (server, addr) = start(|_| {});
    let mut gone = std::net::TcpStream::connect(&addr).unwrap();
    let req = Request::ClaimNextJob {
        channel_id: 1,
        lease_ms: 300,
        wait_ms: 5_000,
    };
    solid_engine_protocol::write_frame(&mut gone, &req.to_frame(1)).unwrap();
    std::thread::sleep(Duration::from_millis(100));
    drop(gone);
    std::thread::sleep(Duration::from_millis(100));
    let mut c = Client::connect_tcp(&addr).unwrap();
    c.call(&push(7, 0)).unwrap();
    let job = claim(&mut c, 3_000).expect("job redelivered once the dead waiter's lease expires");
    assert_eq!(job.job_id, 7);
    server.shutdown();
}

#[test]
fn two_waiters_share_two_jobs_without_duplicates() {
    let (server, addr) = start(|_| {});
    let waiters: Vec<_> = (0..2)
        .map(|_| {
            let a = addr.clone();
            std::thread::spawn(move || {
                claim(&mut Client::connect_tcp(&a).unwrap(), 3_000).map(|j| j.job_id)
            })
        })
        .collect();
    std::thread::sleep(Duration::from_millis(150));
    let mut p = Client::connect_tcp(&addr).unwrap();
    p.call(&push(1, 0)).unwrap();
    p.call(&push(2, 0)).unwrap();
    let mut got: Vec<_> = waiters
        .into_iter()
        .map(|w| w.join().unwrap().unwrap())
        .collect();
    got.sort();
    assert_eq!(got, vec![1, 2]);
    server.shutdown();
}

#[test]
fn lease_expiry_redelivers_and_fences_old_worker() {
    let (server, addr) =
        start(|c| c.jobs.retry.strategy = solid_engine_core::config::BackoffStrategy::Immediate);
    let mut w1 = Client::connect_tcp(&addr).unwrap();
    let mut w2 = Client::connect_tcp(&addr).unwrap();
    w1.call(&push(1, 0)).unwrap();
    let Response::Claim(Some(old)) = w1
        .call(&Request::ClaimNextJob {
            channel_id: 1,
            lease_ms: 100,
            wait_ms: 0,
        })
        .unwrap()
    else {
        panic!()
    };
    // Le worker 2 attend : l'expiration du bail réveille l'attente sans polling.
    let new = claim(&mut w2, 3_000).expect("redelivered after lease expiry");
    assert!(new.fencing_token > old.fencing_token);
    assert_eq!(new.attempts, 2);
    let Response::Error(e) = w1.call(&Request::AckJob(reservation(&old))).unwrap() else {
        panic!()
    };
    assert_eq!(e.code, ErrorCode::StaleReservation);
    assert_eq!(
        w2.call(&Request::AckJob(reservation(&new))).unwrap(),
        Response::Ack
    );
    server.shutdown();
}

#[test]
fn delayed_job_and_stats() {
    let (server, addr) = start(|_| {});
    let mut c = Client::connect_tcp(&addr).unwrap();
    c.call(&push(1, 300)).unwrap();
    assert!(claim(&mut c, 0).is_none());
    let Response::Stats(s) = c
        .call(&Request::GetQueueStats {
            channel_id: Some(1),
        })
        .unwrap()
    else {
        panic!()
    };
    assert_eq!((s.scheduled, s.ready), (1, 0));
    assert_eq!(
        c.call(&Request::WaitForJob {
            channel_id: 1,
            timeout_ms: 3_000
        })
        .unwrap(),
        Response::Wait { available: true }
    );
    assert!(claim(&mut c, 0).is_some());
    server.shutdown();
}

#[test]
fn many_workers_never_share_a_job() {
    let (server, addr) = start(|_| {});
    let mut p = Client::connect_tcp(&addr).unwrap();
    const N: u128 = 400;
    for id in 1..=N {
        p.call(&push(id, 0)).unwrap();
    }
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let handles: Vec<_> = (0..8)
        .map(|_| {
            let (addr, seen) = (addr.clone(), seen.clone());
            std::thread::spawn(move || {
                let mut c = Client::connect_tcp(&addr).unwrap();
                while let Some(j) = claim(&mut c, 0) {
                    assert_eq!(
                        c.call(&Request::AckJob(reservation(&j))).unwrap(),
                        Response::Ack
                    );
                    seen.lock().unwrap().push(j.job_id);
                }
            })
        })
        .collect();
    handles.into_iter().for_each(|h| h.join().unwrap());
    let mut ids = seen.lock().unwrap().clone();
    ids.sort();
    assert_eq!(ids, (1..=N).collect::<Vec<_>>());
    server.shutdown();
}

#[test]
fn invalid_frames_are_rejected() {
    let (server, addr) = start(|c| c.limits.max_payload_bytes = 16);
    let mut raw = std::net::TcpStream::connect(&addr).unwrap();
    raw.write_all(&[0xAB; 32]).unwrap();
    let mut buf = Vec::new();
    raw.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    let _ = raw.read_to_end(&mut buf); // le serveur répond une erreur puis ferme

    let mut c = Client::connect_tcp(&addr).unwrap();
    let Response::Error(e) = c
        .call(&Request::PushJob(PushRequest {
            job_id: 1,
            channel_id: 1,
            priority: 0,
            max_attempts: 0,
            delay_ms: 0,
            partition_key: vec![],
            payload: vec![0; 17],
        }))
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(e.code, ErrorCode::PayloadTooLarge);
    server.shutdown();
}

#[test]
fn pipelined_requests_on_one_connection_are_all_answered_by_request_id() {
    use solid_engine_protocol::{read_frame, write_frame};
    let (server, addr) = start(|_| {});
    let mut s = std::net::TcpStream::connect(&addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    const N: u64 = 500;
    for i in 1..=N {
        let req = push(i as u128, 0);
        write_frame(&mut s, &req.to_frame(i)).unwrap();
    }
    let mut seen = std::collections::HashSet::new();
    for _ in 0..N {
        let f = read_frame(&mut s, 1 << 20).unwrap();
        let Response::Push(_) = Response::decode(f.msg_type & 0x7F, &f.body).unwrap() else {
            panic!()
        };
        assert!(seen.insert(f.request_id));
    }
    assert_eq!(seen.len() as u64, N);
    server.shutdown();
}

#[test]
fn batch_claim_and_ack_check_each_reservation() {
    let (_server, addr) = start(|_| {});
    let mut c = Client::connect_tcp(&addr).unwrap();
    for id in 1..=5u128 {
        c.call(&push(id, 0)).unwrap();
    }
    let batch = Request::ClaimBatch {
        channel_id: 1,
        lease_ms: 30_000,
        max: 3,
        wait_ms: 0,
    };
    let Response::ClaimBatch(first) = c.call(&batch).unwrap() else {
        panic!()
    };
    assert_eq!(first.len(), 3);
    let Response::ClaimBatch(rest) = c
        .call(&Request::ClaimBatch {
            channel_id: 1,
            lease_ms: 30_000,
            max: 10,
            wait_ms: 0,
        })
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(rest.len(), 2);
    let Response::ClaimBatch(none) = c.call(&batch).unwrap() else {
        panic!()
    };
    assert!(none.is_empty());

    // Un jeton de fencing faux est rejeté sans empêcher les autres ACK du lot.
    let mut reservations: Vec<_> = first.iter().map(reservation).collect();
    reservations[1].fencing_token += 100;
    let Response::AckBatch(results) = c.call(&Request::AckBatch(reservations)).unwrap() else {
        panic!()
    };
    assert_eq!(results.len(), 3);
    assert!(results[0].is_none() && results[2].is_none());
    assert_eq!(
        results[1].as_ref().unwrap().code,
        ErrorCode::StaleReservation
    );

    let Response::Stats(s) = c
        .call(&Request::GetQueueStats {
            channel_id: Some(1),
        })
        .unwrap()
    else {
        panic!()
    };
    assert_eq!((s.completed, s.claimed), (2, 3));
}

#[test]
fn batch_claim_waits_for_first_job() {
    let (_server, addr) = start(|_| {});
    let mut waiter = Client::connect_tcp(&addr).unwrap();
    let mut producer = Client::connect_tcp(&addr).unwrap();
    let t = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(100));
        producer.call(&push(1, 0)).unwrap();
    });
    let start = Instant::now();
    let Response::ClaimBatch(jobs) = waiter
        .call(&Request::ClaimBatch {
            channel_id: 1,
            lease_ms: 30_000,
            max: 4,
            wait_ms: 5_000,
        })
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(jobs.len(), 1);
    assert!(start.elapsed() < Duration::from_secs(2));
    t.join().unwrap();
}

#[test]
fn subscription_pushes_jobs_within_credits() {
    // Hyperlane n'est pris en charge par le réacteur que dans le mode direct (Fusion).
    if std::env::var("SOLID_ENGINE_REACTORS").is_ok() && std::env::var("SOLID_ENGINE_REACTOR_DIRECT").is_err() {
        return;
    }
    let (_server, addr) = start(|_| {});
    let mut producer = Client::connect_tcp(&addr).unwrap();
    let mut sub = Client::connect_tcp(&addr).unwrap();
    for id in 1..=10u128 {
        producer.call(&push(id, 0)).unwrap();
    }
    let sid = sub
        .send(&Request::Subscribe {
            channel_id: 1,
            lease_ms: 30_000,
            credits: 4,
        })
        .unwrap();
    assert_eq!(sub.recv().unwrap(), (sid, Response::Subscribed));
    let mut got = Vec::new();
    while got.len() < 4 {
        let (id, Response::Dispatch(jobs)) = sub.recv().unwrap() else {
            panic!()
        };
        assert_eq!(id, sid);
        got.extend(jobs);
    }
    assert_eq!(got.len(), 4);

    // Crédits épuisés : rien de plus n'est poussé.
    std::thread::sleep(Duration::from_millis(300));
    let Response::Stats(s) = producer
        .call(&Request::GetQueueStats {
            channel_id: Some(1),
        })
        .unwrap()
    else {
        panic!()
    };
    assert_eq!((s.claimed, s.ready), (4, 6));

    let acks: Vec<_> = got.iter().map(reservation).collect();
    sub.send(&Request::AckBatch(acks)).unwrap();
    sub.send(&Request::Credit { credits: 6 }).unwrap();
    let mut more = 0;
    let mut acked = false;
    while more < 6 || !acked {
        match sub.recv().unwrap().1 {
            Response::Dispatch(jobs) => more += jobs.len(),
            Response::AckBatch(r) => {
                assert!(r.iter().all(Option::is_none));
                acked = true;
            }
            other => panic!("unexpected {other:?}"),
        }
    }
    assert_eq!(more, 6);
}

#[test]
fn subscription_delivers_every_large_job_across_coalesced_writes() {
    if std::env::var("SOLID_ENGINE_REACTORS").is_ok() && std::env::var("SOLID_ENGINE_REACTOR_DIRECT").is_err() {
        return;
    }
    let (_server, addr) = start(|_| {});
    let mut producer = Client::connect_tcp(&addr).unwrap();
    let mut sub = Client::connect_tcp(&addr).unwrap();
    for id in 1..=60u128 {
        let Request::PushJob(mut p) = push(id, 0) else { unreachable!() };
        p.payload = vec![7; 30 * 1024];
        producer.call(&Request::PushJob(p)).unwrap();
    }
    sub.send(&Request::Subscribe { channel_id: 1, lease_ms: 30_000, credits: 60 }).unwrap();
    sub.recv().unwrap();
    let mut seen = std::collections::HashSet::new();
    while seen.len() < 60 {
        let Response::Dispatch(jobs) = sub.recv().unwrap().1 else { panic!("expected dispatch") };
        seen.extend(jobs.iter().map(|j| j.job_id));
    }
    assert_eq!(seen.len(), 60);
}

#[test]
fn subscription_jobs_return_after_disconnect_lease_expiry() {
    // Hyperlane n'est pris en charge par le réacteur que dans le mode direct (Fusion).
    if std::env::var("SOLID_ENGINE_REACTORS").is_ok() && std::env::var("SOLID_ENGINE_REACTOR_DIRECT").is_err() {
        return;
    }
    let (_server, addr) = start(|_| {});
    let mut c = Client::connect_tcp(&addr).unwrap();
    c.call(&push(1, 0)).unwrap();
    let mut sub = Client::connect_tcp(&addr).unwrap();
    sub.send(&Request::Subscribe {
        channel_id: 1,
        lease_ms: 200,
        credits: 1,
    })
    .unwrap();
    sub.recv().unwrap();
    assert!(matches!(sub.recv().unwrap().1, Response::Dispatch(j) if j.len() == 1));
    drop(sub);
    let job = claim(&mut c, 5_000).expect("job redelivered after lease expiry");
    assert_eq!(job.fencing_token, 2);
}

fn push_req(job_id: u128, key: &str, payload: &[u8]) -> PushRequest {
    PushRequest {
        job_id,
        channel_id: 1,
        priority: 0,
        max_attempts: 0,
        delay_ms: 0,
        partition_key: key.as_bytes().to_vec(),
        payload: payload.to_vec(),
    }
}

#[test]
fn push_batch_reports_each_job_and_replays_idempotently() {
    let (_server, addr) = start(|cfg| cfg.limits.max_payload_bytes = 16);
    let mut c = Client::connect_tcp(&addr).unwrap();
    let mut jobs: Vec<PushRequest> = (1..=40u128)
        .map(|id| push_req(id, &format!("k{id}"), b"ok"))
        .collect();
    jobs[2].payload = vec![0; 64];
    let Response::PushBatch(results) = c.call(&Request::PushBatch(jobs.clone())).unwrap() else {
        panic!()
    };
    assert_eq!(results.len(), 40);
    assert_eq!(
        results[2].as_ref().unwrap_err().code,
        ErrorCode::PayloadTooLarge
    );
    for (i, r) in results.iter().enumerate().filter(|(i, _)| *i != 2) {
        let o = r.as_ref().unwrap();
        assert_eq!((o.job_id, o.duplicate), (i as u128 + 1, false));
    }
    // Un rejeu complet ne duplique rien.
    let Response::PushBatch(again) = c.call(&Request::PushBatch(jobs)).unwrap() else {
        panic!()
    };
    assert!(again
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != 2)
        .all(|(_, r)| r.as_ref().unwrap().duplicate));
    let Response::Stats(s) = c
        .call(&Request::GetQueueStats {
            channel_id: Some(1),
        })
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(s.ready, 39);
}

#[test]
fn push_batch_generates_ids_and_rejects_oversized_batches() {
    let (_server, addr) = start(|_| {});
    let mut c = Client::connect_tcp(&addr).unwrap();
    let Response::PushBatch(results) = c
        .call(&Request::PushBatch(vec![
            push_req(0, "", b"a"),
            push_req(0, "", b"b"),
        ]))
        .unwrap()
    else {
        panic!()
    };
    let ids: Vec<u128> = results.iter().map(|r| r.as_ref().unwrap().job_id).collect();
    assert!(ids[0] != 0 && ids[1] != 0 && ids[0] != ids[1]);
    let Response::Error(e) = c
        .call(&Request::PushBatch(
            (1..=257u128).map(|i| push_req(i, "", b"x")).collect(),
        ))
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(e.code, ErrorCode::BadRequest);
    let Response::Stats(s) = c
        .call(&Request::GetQueueStats {
            channel_id: Some(1),
        })
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(s.ready, 2);
    let Response::PushBatch(empty) = c.call(&Request::PushBatch(Vec::new())).unwrap() else {
        panic!()
    };
    assert!(empty.is_empty());
}

#[test]
fn pipelined_ack_batches_are_answered_by_request_id_and_do_not_block_the_reader() {
    let (_server, addr) = start(|_| {});
    let mut c = Client::connect_tcp(&addr).unwrap();
    for id in 1..=6u128 {
        c.call(&push(id, 0)).unwrap();
    }
    let Response::ClaimBatch(jobs) = c
        .call(&Request::ClaimBatch { channel_id: 1, lease_ms: 30_000, max: 6, wait_ms: 0 })
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(jobs.len(), 6);
    let mut bad: Vec<_> = jobs[3..].iter().map(reservation).collect();
    bad[1].fencing_token += 100;
    let a = c.send(&Request::AckBatch(jobs[..3].iter().map(reservation).collect())).unwrap();
    let b = c.send(&Request::AckBatch(bad)).unwrap();
    let ping = c.send(&Request::Ping).unwrap();
    let mut seen = std::collections::HashMap::new();
    while seen.len() < 3 {
        let (id, resp) = c.recv().unwrap();
        seen.insert(id, resp);
    }
    let Some(Response::AckBatch(ra)) = seen.remove(&a) else { panic!() };
    let Some(Response::AckBatch(rb)) = seen.remove(&b) else { panic!() };
    assert!(matches!(seen.remove(&ping), Some(Response::Pong)));
    assert!(ra.iter().all(Option::is_none));
    assert_eq!(rb.len(), 3);
    assert!(rb[0].is_none() && rb[2].is_none());
    assert_eq!(rb[1].as_ref().unwrap().code, ErrorCode::StaleReservation);
}

#[test]
fn acknowledged_batch_survives_restart_in_strict_mode() {
    let dir = std::env::temp_dir().join(format!("solid-engine-ackbatch-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let configure = |cfg: &mut Config| {
        cfg.storage.durability = solid_engine_core::config::Durability::Strict;
        cfg.storage.data_dir = Some(dir.to_string_lossy().into_owned());
    };
    {
        let (server, addr) = start(configure);
        let mut c = Client::connect_tcp(&addr).unwrap();
        for id in 1..=3u128 {
            c.call(&push(id, 0)).unwrap();
        }
        let Response::ClaimBatch(jobs) = c
            .call(&Request::ClaimBatch { channel_id: 1, lease_ms: 30_000, max: 3, wait_ms: 0 })
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(jobs.len(), 3);
        // La réponse n'arrive qu'après la durabilité : un ACK confirmé doit survivre au redémarrage.
        let acks = jobs.iter().filter(|j| j.job_id != 3).map(reservation).collect();
        let Response::AckBatch(r) = c.call(&Request::AckBatch(acks)).unwrap() else { panic!() };
        assert!(r.iter().all(Option::is_none));
        drop(c);
        server.shutdown();
    }
    let (_server, addr) = start(configure);
    let mut c = Client::connect_tcp(&addr).unwrap();
    let job = claim(&mut c, 0).expect("le job non acquitté est redistribué");
    assert_eq!(job.job_id, 3);
    assert!(claim(&mut c, 0).is_none(), "les jobs acquittés ne reviennent pas");
    let _ = std::fs::remove_dir_all(&dir);
}
