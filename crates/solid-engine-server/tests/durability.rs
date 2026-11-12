use solid_engine_core::config::{Config, Durability};
use solid_engine_core::*;
use solid_engine_protocol::client::Client;
use solid_engine_protocol::{Request, Response};
use solid_engine_server::Server;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn temp_dir(tag: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!("solid-engine-{tag}-{}-{}", std::process::id(), now_ms()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn push(id: u128) -> Request {
    Request::PushJob(PushRequest {
        job_id: id,
        channel_id: 1,
        priority: 0,
        max_attempts: 0,
        delay_ms: 0,
        partition_key: vec![],
        payload: id.to_le_bytes().to_vec(),
    })
}

fn reservation(j: &ClaimedJob) -> Reservation {
    Reservation { job_id: j.job_id, reservation_id: j.reservation_id, fencing_token: j.fencing_token }
}

fn config(dir: &PathBuf, durability: Durability) -> Config {
    let mut cfg = Config::default();
    cfg.server.tcp_addr = Some("127.0.0.1:0".into());
    cfg.storage.durability = durability;
    cfg.storage.data_dir = Some(dir.join("data").to_string_lossy().into());
    cfg.storage.snapshot_every_records = 50;
    cfg
}

#[test]
fn restart_preserves_confirmed_jobs_across_snapshots() {
    for (tag, mode) in [("restart", Durability::Strict), ("restart-balanced", Durability::Balanced)] {
        restart_across_snapshots(tag, mode);
    }
}

// Les snapshots sont écrits par le thread WAL pendant que le shard continue : l'état reconstruit doit rester exact.
fn restart_across_snapshots(tag: &str, mode: Durability) {
    let dir = temp_dir(tag);
    {
        let server = Server::start(config(&dir, mode)).unwrap();
        let mut c = Client::connect_tcp(server.tcp_addr().unwrap()).unwrap();
        // Plus de 50 enregistrements par shard : au moins un snapshot est pris en cours de route.
        for id in 1..=300u128 {
            assert!(matches!(c.call(&push(id)).unwrap(), Response::Push(_)));
        }
        server.shutdown();
    }
    for _ in 0..2 {
        let server = Server::start(config(&dir, mode)).unwrap();
        let mut c = Client::connect_tcp(server.tcp_addr().unwrap()).unwrap();
        let Response::Stats(s) = c.call(&Request::GetQueueStats { channel_id: Some(1) }).unwrap() else { panic!() };
        assert_eq!(s.ready, 300);
        server.shutdown();
    }
}

#[test]
fn data_dir_cannot_be_shared_or_resharded() {
    let dir = temp_dir("locked");
    let server = Server::start(config(&dir, Durability::Strict)).unwrap();
    assert!(Server::start(config(&dir, Durability::Strict)).is_err());
    server.shutdown();
    let mut cfg = config(&dir, Durability::Strict);
    cfg.engine.shards = 2;
    assert!(Server::start(cfg).is_err());
}

struct Daemon {
    child: Child,
    sock: PathBuf,
}

impl Daemon {
    fn start(dir: &PathBuf) -> Self {
        Self::start_with(dir, 100_000)
    }

    fn start_with(dir: &PathBuf, snapshot_every_records: u64) -> Self {
        Self::start_cfg(dir, snapshot_every_records, "strict", false)
    }

    fn start_cfg(dir: &PathBuf, snapshot_every_records: u64, durability: &str, single_flight: bool) -> Self {
        // Nom court : `sun_path` est limité à 104 octets sur macOS et `$TMPDIR` y est déjà long.
        let sock = dir.join("e.sock");
        let cfg_path = dir.join("engine.toml");
        let toml = format!(
            "[engine]\nshards = 2\n\n[server]\ntcp_addr = \"127.0.0.1:0\"\nunix_socket = \"{}\"\n\n\
             [storage]\ndurability = \"{durability}\"\ndata_dir = \"{}\"\nsnapshot_every_records = {}\nsnapshot_single_flight = {single_flight}\n",
            sock.display(),
            dir.join("data").display(),
            snapshot_every_records
        );
        std::fs::write(&cfg_path, toml).unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_solid-engine-server"))
            .arg("--config")
            .arg(&cfg_path)
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        // Le socket n'est lié qu'une fois la récupération terminée.
        let deadline = Instant::now() + Duration::from_secs(10);
        while Client::connect_unix(&sock).is_err() {
            assert!(Instant::now() < deadline, "server did not start");
            std::thread::sleep(Duration::from_millis(20));
        }
        Self { child, sock }
    }

    fn client(&self) -> Client<std::os::unix::net::UnixStream> {
        Client::connect_unix(&self.sock).unwrap()
    }

    fn sigkill(mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
    }
}

#[test]
fn sigkill_keeps_confirmed_pushes_and_fences_the_old_worker() {
    let dir = temp_dir("sigkill");
    let daemon = Daemon::start(&dir);
    let mut c = daemon.client();
    for id in 1..=20u128 {
        assert!(matches!(c.call(&push(id)).unwrap(), Response::Push(_)));
    }
    let Response::Claim(Some(old)) =
        c.call(&Request::ClaimNextJob { channel_id: 1, lease_ms: 600_000, wait_ms: 0 }).unwrap()
    else {
        panic!()
    };
    let Response::Claim(Some(acked)) =
        c.call(&Request::ClaimNextJob { channel_id: 1, lease_ms: 600_000, wait_ms: 0 }).unwrap()
    else {
        panic!()
    };
    assert_eq!(c.call(&Request::AckJob(reservation(&acked))).unwrap(), Response::Ack);
    daemon.sigkill();

    // Redémarrages répétés après un SIGKILL.
    for round in 0..2 {
        let daemon = Daemon::start(&dir);
        let mut c = daemon.client();
        let Response::Stats(s) = c.call(&Request::GetQueueStats { channel_id: Some(1) }).unwrap() else { panic!() };
        assert_eq!((s.ready, s.claimed, s.completed), (19, 0, 1), "round {round}");
        if round == 0 {
            // Le job réservé avant le crash est de nouveau disponible ; l'ancien worker est rejeté.
            let Response::Error(e) = c.call(&Request::AckJob(reservation(&old))).unwrap() else { panic!() };
            assert_eq!(e.code, ErrorCode::StaleReservation);
            let Response::Job(v) = c.call(&Request::GetJob { job_id: old.job_id }).unwrap() else { panic!() };
            assert_eq!((v.state, v.attempts, v.fencing_token), (JobState::Ready, 1, old.fencing_token));
        }
        daemon.sigkill();
    }
}

fn count_files(dir: &std::path::Path, pred: &dyn Fn(&std::path::Path) -> bool) -> usize {
    let Ok(rd) = std::fs::read_dir(dir) else { return 0 };
    rd.flatten()
        .map(|e| {
            let p = e.path();
            if p.is_dir() { count_files(&p, pred) } else { usize::from(pred(&p)) }
        })
        .sum()
}

/// Vrai SIGKILL pendant que des snapshots sont écrits en continu (un tous les 20 enregistrements par shard) :
/// à chaque redémarrage, tout job dont le push a été confirmé doit exister, et rien d'autre que des pushes envoyés.
#[test]
fn sigkill_during_continuous_snapshots_loses_no_confirmed_push() {
    use std::sync::{Arc, Mutex};
    let dir = temp_dir("sigkill-snap");
    let confirmed: Arc<Mutex<Vec<u128>>> = Arc::new(Mutex::new(Vec::new()));
    let (mut next_id, mut sent_max) = (1u128, 0u128);
    let (mut with_tmp, mut with_snaps) = (0, 0);
    for iter in 0..25u64 {
        let daemon = Daemon::start_with(&dir, 20);
        {
            // Après récupération : tout job confirmé est présent, et le total ne dépasse pas ce qui a été envoyé.
            let mut c = daemon.client();
            let ids = confirmed.lock().unwrap().clone();
            for id in &ids {
                let r = c.call(&Request::GetJob { job_id: *id }).unwrap();
                assert!(matches!(r, Response::Job(_)), "iter {iter}: job confirmé {id} perdu : {r:?}");
            }
            let Response::Stats(s) = c.call(&Request::GetQueueStats { channel_id: Some(1) }).unwrap() else { panic!() };
            let total = (s.ready + s.claimed + s.completed) as u128;
            assert!(total >= ids.len() as u128 && total <= sent_max, "iter {iter}: total {total}, confirmés {}, envoyés {sent_max}", ids.len());
        }
        let handles: Vec<_> = (0..4u128)
            .map(|t| {
                let (mut c, confirmed) = (daemon.client(), confirmed.clone());
                let base = next_id + t * 100_000;
                std::thread::spawn(move || {
                    for k in 0..100_000u128 {
                        match c.call(&push(base + k)) {
                            Ok(Response::Push(_)) => confirmed.lock().unwrap().push(base + k),
                            _ => return base + k,
                        }
                    }
                    base + 100_000
                })
            })
            .collect();
        std::thread::sleep(Duration::from_millis(40 + (iter * 53) % 260));
        let data = dir.join("data");
        with_tmp += usize::from(count_files(&data, &|p| p.extension().is_some_and(|e| e == "tmp")) > 0);
        with_snaps += usize::from(count_files(&data, &|p| p.extension().is_some_and(|e| e == "snap")) > 0);
        daemon.sigkill();
        for h in handles {
            // Le dernier identifiant tenté par ce thread a pu être écrit sans confirmation.
            sent_max = sent_max.max(h.join().unwrap());
        }
        next_id += 4 * 100_000;
    }
    println!(
        "sigkill_snap: confirmés={} kills avec .tmp visible={with_tmp} avec snapshot publié={with_snaps}",
        confirmed.lock().unwrap().len()
    );
}

const ORACLE_SHARDS: usize = 2;
const ORACLE_WORDS: usize = 1 << 20;
const ID_LIMIT: u128 = 1 << 26;

fn crc_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    for (i, t) in table.iter_mut().enumerate() {
        let mut c = i as u32;
        for _ in 0..8 {
            c = if c & 1 == 1 { 0xEDB8_8320 ^ (c >> 1) } else { c >> 1 };
        }
        *t = c;
    }
    table
}

fn crc32(table: &[u32; 256], data: &[u8]) -> u32 {
    !data.iter().fold(!0u32, |c, &x| table[((c ^ x as u32) & 0xff) as usize] ^ (c >> 8))
}

fn bit_set(bits: &mut [u64], id: u128) {
    bits[(id >> 6) as usize] |= 1 << (id & 63);
}

fn bit_get(bits: &[u64], id: u128) -> bool {
    bits[(id >> 6) as usize] >> (id & 63) & 1 == 1
}

/// Ensemble récupérable d'après le disque après un SIGKILL. Oracle volontairement indépendant de `recover_shard` : CRC32,
/// format de snapshot et de record réimplémentés ici. Règle : dernier snapshot valide, puis records WAL de seq consécutive
/// jusqu'au premier record invalide, tronqué ou rupture de séquence. Ce scénario ne fait que des `PushJob` : tout autre
/// type de record, tout état autre que `Ready` ou tout canal/payload inattendu est compté dans `unexpected`
/// (pas de transition ni de rétention à modéliser ici).
struct Recoverable {
    ids: Vec<u64>,
    unexpected: u64,
    bad_records: u64,
    forge_target: Option<(PathBuf, usize, usize)>,
}

fn recoverable_from_disk(data: &std::path::Path) -> Recoverable {
    let table = crc_table();
    let mut r = Recoverable { ids: vec![0; ORACLE_WORDS], unexpected: 0, bad_records: 0, forge_target: None };
    for sh in 0..ORACLE_SHARDS {
        let Ok(rd) = std::fs::read_dir(data.join(format!("shard-{sh}"))) else { continue };
        let (mut snaps, mut logs) = (Vec::new(), Vec::new());
        for p in rd.flatten().map(|e| e.path()) {
            let name = p.file_name().unwrap().to_string_lossy().into_owned();
            if name.starts_with("snap-") && name.ends_with(".snap") {
                snaps.push(p);
            } else if name.starts_with("wal-") && name.ends_with(".log") {
                logs.push(p);
            }
        }
        snaps.sort();
        logs.sort();
        let mut snap_seq = 0u64;
        for p in snaps.iter().rev() {
            let Ok(b) = std::fs::read(p) else { continue };
            if b.len() < 30 + 4 {
                continue;
            }
            let (body, crc) = b.split_at(b.len() - 4);
            if crc32(&table, body) != u32::from_le_bytes(crc.try_into().unwrap()) {
                r.bad_records += 1;
                continue;
            }
            snap_seq = u64::from_le_bytes(body[10..18].try_into().unwrap());
            let st = &body[26..];
            let n = u64::from_le_bytes(st[16..24].try_into().unwrap()) as usize;
            let mut off = 24;
            for _ in 0..n {
                let id = u128::from_le_bytes(st[off..off + 16].try_into().unwrap());
                let channel = u16::from_le_bytes(st[off + 16..off + 18].try_into().unwrap());
                let state = st[off + 18];
                let plen = u32::from_le_bytes(st[off + 88..off + 92].try_into().unwrap()) as usize;
                if id < ID_LIMIT && channel == 1 && state == JobState::Ready as u8 && plen == 16 {
                    bit_set(&mut r.ids, id);
                } else {
                    r.unexpected += 1;
                }
                off += 92 + plen;
            }
            break;
        }
        let mut expected = snap_seq + 1;
        'segs: for p in &logs {
            let Ok(b) = std::fs::read(p) else { continue };
            let mut off = 8;
            while off + 36 <= b.len() {
                let total = 32 + u32::from_le_bytes(b[off..off + 4].try_into().unwrap()) as usize + 4;
                if off + total > b.len() {
                    break;
                }
                let crc = u32::from_le_bytes(b[off + total - 4..off + total].try_into().unwrap());
                if crc32(&table, &b[off..off + total - 4]) != crc {
                    r.bad_records += 1;
                    break 'segs;
                }
                let seq = u64::from_le_bytes(b[off + 8..off + 16].try_into().unwrap());
                if seq >= expected {
                    if seq != expected {
                        break 'segs;
                    }
                    expected += 1;
                    let id = u128::from_le_bytes(b[off + 16..off + 32].try_into().unwrap());
                    if b[off + 5] == 1 && id < ID_LIMIT {
                        bit_set(&mut r.ids, id);
                        if sh == 0 {
                            r.forge_target = Some((p.clone(), off, total));
                        }
                    } else {
                        r.unexpected += 1;
                    }
                }
                off += total;
            }
        }
    }
    r
}

/// Contrôles négatifs (variable `INV_MUTATE`, à n'utiliser que pour démontrer que les invariants échouent) :
/// - `trunc` : tronque le dernier segment WAL du shard 0 après la lecture de l'oracle ;
/// - `forge` : réécrit l'identifiant du dernier record rejoué du shard 0 AVEC un CRC valide, donc sans fin de WAL apparente :
///   le nombre de jobs récupérés reste identique, seul l'ensemble exact des identifiants diffère ;
/// - `oracle` : retire un identifiant de l'oracle (vérifie que la comparaison détecte aussi un écart dans ce sens).
fn apply_mutation(kind: &str, data: &std::path::Path, oracle: &mut Recoverable) {
    match kind {
        "trunc" => {
            let mut logs: Vec<_> = std::fs::read_dir(data.join("shard-0"))
                .unwrap()
                .flatten()
                .map(|e| e.path())
                .filter(|p| p.extension().is_some_and(|x| x == "log"))
                .collect();
            logs.sort();
            if let Some(p) = logs.last() {
                let f = std::fs::OpenOptions::new().write(true).open(p).unwrap();
                let len = f.metadata().unwrap().len();
                f.set_len(8 + len.saturating_sub(8) / 2).unwrap();
            }
        }
        "forge" => {
            if let Some((p, off, total)) = oracle.forge_target.clone() {
                let mut b = std::fs::read(&p).unwrap();
                let id = u128::from_le_bytes(b[off + 16..off + 32].try_into().unwrap());
                b[off + 16..off + 32].copy_from_slice(&(id + (1 << 25)).to_le_bytes());
                let crc = crc32(&crc_table(), &b[off..off + total - 4]);
                b[off + total - 4..off + total].copy_from_slice(&crc.to_le_bytes());
                std::fs::write(&p, b).unwrap();
            }
        }
        "oracle" => {
            if let Some(w) = oracle.ids.iter_mut().rev().find(|w| **w != 0) {
                *w &= *w - 1;
            }
        }
        other => panic!("INV_MUTATE inconnu : {other}"),
    }
}

/// Même scénario en `balanced` avec un seul snapshot en vol par shard. L'acquittement précède l'écriture et le fsync du WAL :
/// un job acquitté peut disparaître avec le dernier lot non écrit. Le pourcentage de pertes est donc un indicateur
/// (performance, risque opérationnel) affiché mais n'est pas un critère de correction : il dépend de la plateforme
/// (≈0,6 % sous Linux, ≈4 % sous macOS, jusqu'à ≈8 % observé). La correction est vérifiée par des invariants calculés
/// contre un oracle lu sur le disque après chaque SIGKILL (voir `recoverable_from_disk`) :
///  I1 tout Push acquitté dont le record est récupérable est retrouvé, à l'état exact (Ready, canal 1, payload 16 octets) ;
///  I2 toute perte d'un Push acquitté correspond à un record absent du préfixe récupérable ;
///  I3 aucun job retrouvé sans record récupérable ;
///  I4 l'ensemble récupéré est exactement l'ensemble récupérable (chaque identifiant du disque est retrouvé, et le total
///     des jobs du moteur est égal à la taille de cet ensemble : ni ajout, ni omission) ;
/// et les jobs récupérés sans ACK client (requête en vol au kill) sont comptés à part, au plus un par producteur et par kill.
#[test]
fn sigkill_during_continuous_snapshots_balanced_single_flight_recovers_consistently() {
    use std::sync::{Arc, Mutex};
    let mutation = std::env::var("INV_MUTATE").ok();
    let dir = temp_dir("kill-bal");
    let confirmed: Arc<Mutex<Vec<u128>>> = Arc::new(Mutex::new(Vec::new()));
    let (mut next_id, mut sent_max) = (1u128, 0u128);
    let (mut lost_total, mut checked_total) = (0usize, 0usize);
    let (mut i1, mut i2, mut i3, mut i4, mut unexpected, mut unacked_excess) = (0u64, 0u64, 0u64, 0u64, 0u64, 0u64);
    let (mut unacked_total, mut bad_records) = (0u64, 0u64);
    let mut known_unacked = vec![0u64; ORACLE_WORDS];
    let mut prev: Option<Recoverable> = None;
    for iter in 0..12u64 {
        let daemon = Daemon::start_cfg(&dir, 20, "balanced", true);
        if let Some(oracle) = prev.take() {
            let mut c = daemon.client();
            let mut ids = confirmed.lock().unwrap();
            let mut kept = Vec::with_capacity(ids.len());
            let mut conf_bits = vec![0u64; ORACLE_WORDS];
            for id in ids.iter() {
                bit_set(&mut conf_bits, *id);
                let on_disk = bit_get(&oracle.ids, *id);
                match c.call(&Request::GetJob { job_id: *id }).unwrap() {
                    Response::Job(v) => {
                        kept.push(*id);
                        let exact = v.job_id == *id
                            && v.channel_id == 1
                            && v.state == JobState::Ready
                            && v.attempts == 0
                            && v.payload_length == 16;
                        if !on_disk {
                            i3 += 1;
                        } else if !exact {
                            i1 += 1;
                        }
                    }
                    _ => {
                        lost_total += 1;
                        if on_disk {
                            i1 += 1;
                        } else {
                            i2 += 1;
                        }
                    }
                }
            }
            // Records valides sur disque sans ACK client : ils doivent être retrouvés (I4), mais ne sont pas des acquittements.
            let mut unacked_now = 0u64;
            for (w, word) in oracle.ids.iter().enumerate() {
                let mut rest = word & !conf_bits[w];
                while rest != 0 {
                    let id = (w as u128) * 64 + rest.trailing_zeros() as u128;
                    rest &= rest - 1;
                    match c.call(&Request::GetJob { job_id: id }).unwrap() {
                        Response::Job(_) => {
                            if !bit_get(&known_unacked, id) {
                                bit_set(&mut known_unacked, id);
                                unacked_now += 1;
                            }
                        }
                        _ => i4 += 1,
                    }
                }
            }
            checked_total += ids.len();
            *ids = kept;
            let Response::Stats(s) = c.call(&Request::GetQueueStats { channel_id: Some(1) }).unwrap() else { panic!() };
            let total = (s.ready + s.claimed + s.completed) as u128;
            assert!(total <= sent_max, "iter {iter}: {total} jobs récupérés pour {sent_max} envoyés");
            let disk_total: u64 = oracle.ids.iter().map(|w| w.count_ones() as u64).sum();
            i4 += (total as u64).abs_diff(disk_total);
            unexpected += oracle.unexpected;
            bad_records += oracle.bad_records;
            unacked_total += unacked_now;
            if unacked_now > 4 {
                unacked_excess += 1;
            }
        }
        let handles: Vec<_> = (0..4u128)
            .map(|t| {
                let (mut c, confirmed) = (daemon.client(), confirmed.clone());
                let base = next_id + t * 100_000;
                std::thread::spawn(move || {
                    for k in 0..100_000u128 {
                        match c.call(&push(base + k)) {
                            Ok(Response::Push(_)) => confirmed.lock().unwrap().push(base + k),
                            _ => return base + k,
                        }
                    }
                    base + 100_000
                })
            })
            .collect();
        std::thread::sleep(Duration::from_millis(40 + (iter * 53) % 260));
        daemon.sigkill();
        for h in handles {
            sent_max = sent_max.max(h.join().unwrap());
        }
        let mut oracle = recoverable_from_disk(&dir.join("data"));
        if let Some(kind) = &mutation {
            apply_mutation(kind, &dir.join("data"), &mut oracle);
        }
        prev = Some(oracle);
        next_id += 4 * 100_000;
    }
    println!(
        "sigkill_snap_balanced: vérifiés={checked_total} perdus={lost_total} ({:.2} %, indicateur non bloquant) \
         expliqués_par_absence_de_record={i2} sans_ack_client_récupérés={unacked_total} crc_invalides={bad_records} \
         I1={i1} I3={i3} I4={i4} inattendus={unexpected}",
        lost_total as f64 * 100.0 / checked_total.max(1) as f64
    );
    assert_eq!(i1, 0, "I1 : Push acquitté dont le record est récupérable non retrouvé ou dans un état inexact");
    assert_eq!(lost_total as u64, i2, "I2 : perte non expliquée par l'absence de record");
    assert_eq!(i3, 0, "I3 : job retrouvé sans record récupérable");
    assert_eq!(i4, 0, "I4 : ensemble récupéré différent de l'ensemble récupérable");
    assert_eq!(unexpected, 0, "record ou état inattendu dans l'oracle");
    assert_eq!(unacked_excess, 0, "plus de 4 jobs récupérés sans ACK client pour un kill");
}
