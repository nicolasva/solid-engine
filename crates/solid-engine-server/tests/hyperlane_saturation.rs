use solid_engine_core::config::{Config, Durability};
use solid_engine_core::{PushRequest, Reservation};
use solid_engine_protocol::{read_frame, write_frame, Request, Response};
use solid_engine_server::Server;
use std::io::BufWriter;
use std::net::TcpStream;
use std::time::Duration;

const PINGS: u64 = 300_000;
const LIMIT: usize = 64;

fn push(id: u128) -> Request {
    Request::PushJob(PushRequest {
        job_id: id,
        channel_id: 1,
        priority: 0,
        max_attempts: 0,
        delay_ms: 0,
        partition_key: vec![],
        payload: vec![0u8; 16],
    })
}

/// Une souscription Hyperlane dont le client cesse de lire pendant qu'il inonde la connexion de requêtes :
/// la file de réponses atteint la limite et le lecteur suspend la lecture. Dès que le client reprend la lecture,
/// les `Pong` et les `Dispatch` en attente doivent arriver, puis un `AckBatch` et un `Credit` envoyés ensuite
/// doivent progresser (timeout de lecture : un blocage fait échouer le test au lieu de le suspendre).
#[test]
fn subscription_progresses_after_response_queue_saturation() {
    // Hyperlane n'est pris en charge par le réacteur que dans le mode direct (Fusion).
    if std::env::var("SOLID_ENGINE_REACTORS").is_ok() && std::env::var("SOLID_ENGINE_REACTOR_DIRECT").is_err() {
        return;
    }
    // Lu une seule fois par processus : ce fichier ne contient qu'un test.
    std::env::set_var("SOLID_ENGINE_OUT_LIMIT", LIMIT.to_string());
    let mut cfg = Config::default();
    cfg.server.tcp_addr = Some("127.0.0.1:0".into());
    cfg.storage.durability = Durability::Memory;
    let server = Server::start(cfg).unwrap();
    let addr = server.tcp_addr().unwrap();

    let mut producer = TcpStream::connect(addr).unwrap();
    for id in 1..=2000u128 {
        write_frame(&mut producer, &push(id).to_frame(id as u64)).unwrap();
        read_frame(&mut producer, 1 << 20).unwrap();
    }

    let stream = TcpStream::connect(addr).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(20))).unwrap();
    let mut reader = stream.try_clone().unwrap();
    let mut writer = stream.try_clone().unwrap();
    const SUB_ID: u64 = u64::MAX;
    write_frame(&mut writer, &Request::Subscribe { channel_id: 1, lease_ms: 60_000, credits: 200 }.to_frame(SUB_ID))
        .unwrap();
    let flood = std::thread::spawn(move || {
        let mut w = BufWriter::new(writer);
        for id in 1..=PINGS {
            write_frame(&mut w, &Request::Ping.to_frame(id)).unwrap();
        }
        write_frame(&mut w, &Request::Credit { credits: 100 }.to_frame(SUB_ID - 1)).unwrap();
        std::io::Write::flush(&mut w).unwrap();
        w
    });
    // Lecture suspendue : la file de réponses se remplit jusqu'à la limite.
    std::thread::sleep(Duration::from_secs(3));

    let (mut pongs, mut jobs, mut reservations) = (0u64, 0usize, Vec::new());
    while pongs < PINGS || jobs < 200 {
        let f = read_frame(&mut reader, 1 << 20).expect("la connexion ne progresse plus après saturation");
        match Response::decode(f.msg_type, &f.body).unwrap() {
            Response::Pong => pongs += 1,
            Response::Subscribed => {}
            Response::Dispatch(js) => {
                jobs += js.len();
                reservations.extend(js.iter().map(|j| Reservation {
                    job_id: j.job_id,
                    reservation_id: j.reservation_id,
                    fencing_token: j.fencing_token,
                }));
            }
            other => panic!("réponse inattendue : {other:?}"),
        }
    }
    let mut writer = flood.join().unwrap().into_inner().unwrap();
    // Le `Credit` envoyé derrière l'inondation est traité une fois la file résorbée : 100 jobs de plus arrivent.
    // L'`AckBatch` qui suit doit lui aussi être traité et répondu.
    write_frame(&mut writer, &Request::AckBatch(reservations.clone()).to_frame(42)).unwrap();
    let (mut acked, mut more) = (false, 0usize);
    while !acked || jobs + more < 300 {
        let f = read_frame(&mut reader, 1 << 20).expect("AckBatch ou Credit bloqué derrière la saturation");
        match Response::decode(f.msg_type, &f.body).unwrap() {
            Response::AckBatch(r) => {
                assert_eq!(f.request_id, 42);
                assert!(r.iter().all(Option::is_none), "ack refusé");
                acked = true;
            }
            Response::Dispatch(js) => more += js.len(),
            other => panic!("réponse inattendue : {other:?}"),
        }
    }
    let peak = solid_engine_server::net::out_queue_peak();
    assert!(peak <= LIMIT + 1024, "file writer non bornée : pic de {peak}");
    println!("hyperlane_saturation: peak_queue={peak} jobs_dispatched={}", jobs + more);
    server.shutdown();
}
