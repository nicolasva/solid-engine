use crate::config::{BackoffStrategy, Config};
use crate::done_table::DoneTable;
use crate::record::{Rd, Record, Wr};
use crate::*;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashMap, VecDeque};
use std::sync::Arc;

const STATES: usize = 6;
const RETENTION_SLOT_SHIFT: u32 = 7;
const RESERVATION_MASK: u64 = (1 << 48) - 1;

struct Job {
    id: u128,
    channel_id: u16,
    state: JobState,
    priority: u8,
    attempts: u32,
    max_attempts: u32,
    fencing_token: u64,
    reservation_id: u64,
    lease_deadline: u64,
    scheduled_at: u64,
    created_at: u64,
    /// Invalide les entrées périmées des files et des heaps (suppression paresseuse).
    seq: u64,
    /// Longueur d'origine, conservée après la libération du payload d'un job terminal.
    payload_len: u32,
    payload: Vec<u8>,
}

/// Trace d'un job terminal (dédoublonnage, `get`, fencing) : sans payload, bail ni réservation.
#[derive(Clone, Copy)]
pub(crate) struct Done {
    pub(crate) fencing_token: u64,
    pub(crate) purge_at: u64,
    /// `created_at` et `scheduled_at` stockés comme écart (ms) en dessous de `purge_at` ; `WIDE` renvoie à `Shard::wide`.
    pub(crate) created_off: u32,
    pub(crate) scheduled_off: u32,
    pub(crate) attempts: u32,
    pub(crate) max_attempts: u32,
    pub(crate) payload_len: u32,
    pub(crate) channel_id: u16,
    pub(crate) state: JobState,
    pub(crate) priority: u8,
}

const WIDE: u32 = u32::MAX;

impl Done {
    pub(crate) const EMPTY: Done = Done {
        fencing_token: 0,
        purge_at: 0,
        created_off: 0,
        scheduled_off: 0,
        attempts: 0,
        max_attempts: 0,
        payload_len: 0,
        channel_id: 0,
        state: JobState::Ready,
        priority: 0,
    };
}

/// Clé alignée sur 8 octets : un `u128` impose 16 et gonfle l'emplacement de la table `done` (56 o au lieu de 64).
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "slot64", repr(align(16)))]
pub(crate) struct Id(pub(crate) [u64; 2]);

impl Id {
    fn new(v: u128) -> Self {
        Id([v as u64, (v >> 64) as u64])
    }
    fn get(self) -> u128 {
        self.0[0] as u128 | (self.0[1] as u128) << 64
    }
}

fn offset_below(purge_at: u64, t: u64) -> Option<u32> {
    purge_at.checked_sub(t).filter(|d| *d < WIDE as u64).map(|d| d as u32)
}

impl Done {
    /// Renvoie aussi les horodatages exacts quand l'un des deux ne tient pas dans un écart u32 (rare : > 49 jours ou après `purge_at`).
    fn from_job(j: &Job, purge_at: u64) -> (Self, Option<(u64, u64)>) {
        let offs = offset_below(purge_at, j.created_at).zip(offset_below(purge_at, j.scheduled_at));
        let (created_off, scheduled_off) = offs.unwrap_or((WIDE, WIDE));
        let d = Done {
            fencing_token: j.fencing_token,
            purge_at,
            created_off,
            scheduled_off,
            attempts: j.attempts,
            max_attempts: j.max_attempts,
            payload_len: j.payload_len,
            channel_id: j.channel_id,
            state: j.state,
            priority: j.priority,
        };
        (d, offs.is_none().then_some((j.created_at, j.scheduled_at)))
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct MemReport {
    pub active_jobs: usize,
    pub active_capacity: usize,
    pub done_entries: usize,
    pub done_capacity: usize,
    pub done_entry_size: usize,
    pub done_slot_size: usize,
    pub done_table_bytes: usize,
    pub retention_slices: usize,
    pub retention_entries: usize,
    pub retention_capacity: usize,
    pub retention_bytes: usize,
    pub done_rehashes: u64,
    pub done_rehash_max_us: u64,
    pub done_rehash_total_us: u64,
    pub done_max_probe: u32,
    pub done_shrinks: u64,
    pub done_shrink_max_us: u64,
}

#[derive(Default)]
struct Queue {
    ready: BTreeMap<u8, VecDeque<(u128, u64)>>,
}

const LEASES_COMPACT_MIN: usize = 4096;

type Deadline = Reverse<(u64, u128, u64)>;

/// État exclusif d'un shard. Un seul thread le modifie : aucun verrou interne.
/// Toutes les méthodes reçoivent `now` (ms UTC) pour rester déterministes.
pub struct Shard {
    id: u16,
    cfg: Arc<Config>,
    jobs: HashMap<u128, Job>,
    done: DoneTable,
    last_tick: u64,
    shrink_pending: bool,
    shrink_finished: bool,
    /// Horodatages exacts des rares jobs terminés dont l'écart à `purge_at` ne tient pas dans un u32.
    wide: HashMap<Id, (u64, u64)>,
    queues: HashMap<u16, Queue>,
    counts: HashMap<u16, [u64; STATES]>,
    scheduled: BinaryHeap<Deadline>,
    leases: BinaryHeap<Deadline>,
    /// Identifiants des jobs terminaux par tranche d'échéance (`purge_at >> RETENTION_SLOT_SHIFT`), dans l'ordre d'insertion.
    retention: BTreeMap<u64, VecDeque<u128>>,
    retention_len: usize,
    /// Taille du tas de leases au-delà de laquelle on le compacte (les ACK laissent des entrées périmées).
    leases_compact_at: usize,
    next_seq: u64,
    next_reservation: u64,
    rng: u64,
    counters: Stats,
    became_ready: bool,
    logging: bool,
    log: Vec<Record>,
}

impl Shard {
    pub fn new(id: u16, cfg: Arc<Config>) -> Self {
        Self {
            id,
            cfg,
            jobs: HashMap::new(),
            done: DoneTable::default(),
            last_tick: 0,
            shrink_pending: false,
            shrink_finished: false,
            wide: HashMap::new(),
            queues: HashMap::new(),
            counts: HashMap::new(),
            scheduled: BinaryHeap::new(),
            leases: BinaryHeap::new(),
            retention: BTreeMap::new(),
            retention_len: 0,
            leases_compact_at: LEASES_COMPACT_MIN,
            next_seq: 1,
            next_reservation: 1,
            rng: 0x9E37_79B9_7F4A_7C15 ^ ((id as u64 + 1) << 32) ^ now_ms(),
            counters: Stats::default(),
            became_ready: false,
            logging: false,
            log: Vec::new(),
        }
    }

    /// Active l'émission des enregistrements WAL (désactivée en mode mémoire et pendant le replay).
    pub fn set_logging(&mut self, on: bool) {
        self.logging = on;
    }

    /// Enregistrements produits depuis le dernier appel, dans l'ordre d'application.
    pub fn take_log(&mut self) -> Vec<Record> {
        std::mem::take(&mut self.log)
    }

    /// Applique une mutation et la journalise : un seul chemin pour le live et le replay.
    fn commit(&mut self, rec: Record) {
        self.apply(&rec);
        if self.logging {
            self.log.push(rec);
        }
    }

    /// Rejoue un enregistrement (récupération). Ne valide rien : le WAL est la source de vérité.
    pub fn apply(&mut self, rec: &Record) {
        match rec {
            Record::Push { job_id, channel_id, priority, max_attempts, created_at, scheduled_at, payload } => {
                let job = Job {
                    id: *job_id,
                    channel_id: *channel_id,
                    state: JobState::Ready,
                    priority: *priority,
                    attempts: 0,
                    max_attempts: *max_attempts,
                    fencing_token: 0,
                    reservation_id: 0,
                    lease_deadline: 0,
                    scheduled_at: *scheduled_at,
                    created_at: *created_at,
                    seq: 0,
                    payload_len: payload.len() as u32,
                    payload: payload.clone(),
                };
                self.bump(*channel_id, JobState::Ready, 1);
                self.jobs.insert(*job_id, job);
                if scheduled_at > created_at {
                    self.schedule(*job_id, *scheduled_at);
                } else {
                    self.make_ready(*job_id);
                }
            }
            Record::Claim { job_id, reservation_id, fencing_token, lease_deadline } => {
                let Some(job) = self.jobs.get_mut(job_id) else { return };
                job.attempts += 1;
                job.fencing_token = *fencing_token;
                job.reservation_id = *reservation_id;
                job.lease_deadline = *lease_deadline;
                self.next_reservation = self.next_reservation.max((reservation_id & RESERVATION_MASK) + 1);
                self.set_state(*job_id, JobState::Claimed);
                self.leases.push(Reverse((*lease_deadline, *job_id, *fencing_token)));
            }
            Record::Extend { job_id, lease_deadline } => {
                let Some(job) = self.jobs.get_mut(job_id) else { return };
                job.lease_deadline = job.lease_deadline.max(*lease_deadline);
                let (deadline, fence) = (job.lease_deadline, job.fencing_token);
                self.leases.push(Reverse((deadline, *job_id, fence)));
            }
            Record::Ack { job_id, at } => {
                if self.jobs.contains_key(job_id) {
                    self.set_state(*job_id, JobState::Completed);
                    self.finish(*job_id, at + self.cfg.jobs.completed_retention_ms);
                }
            }
            Record::Retry { job_id, at, scheduled_at } => {
                let Some(job) = self.jobs.get_mut(job_id) else { return };
                job.lease_deadline = 0;
                job.scheduled_at = *scheduled_at;
                if scheduled_at <= at {
                    self.make_ready(*job_id);
                } else {
                    self.schedule(*job_id, *scheduled_at);
                }
            }
            Record::Dead { job_id, at } => {
                if let Some(job) = self.jobs.get_mut(job_id) {
                    job.lease_deadline = 0;
                    self.set_state(*job_id, JobState::Dead);
                    self.finish(*job_id, at + self.cfg.jobs.dead_retention_ms);
                }
            }
            Record::Cancel { job_id, at } => {
                if self.jobs.contains_key(job_id) {
                    self.set_state(*job_id, JobState::Cancelled);
                    self.finish(*job_id, at + self.cfg.jobs.completed_retention_ms);
                }
            }
        }
    }

    /// Fin de récupération : les réservations de l'ancienne incarnation sont invalidées
    /// (journalisées) avant toute nouvelle attribution, puis les jobs sont replanifiés.
    pub fn finish_recovery(&mut self, now: u64) {
        let claimed: Vec<u128> =
            self.jobs.values().filter(|j| j.state == JobState::Claimed).map(|j| j.id).collect();
        for id in claimed {
            self.retry_or_die(id, Some(0), now);
        }
        self.tick(now);
    }

    /// Prochaine échéance (ms UTC) à traiter, pour dormir sans polling.
    /// Tailles des structures internes, pour diagnostiquer la mémoire :
    /// (jobs, capacité de la table, rétention, leases, planifiés, entrées de file prêtes).
    pub fn sizes(&self) -> [usize; 6] {
        let ready = self.queues.values().map(|q| q.ready.values().map(|d| d.len()).sum::<usize>()).sum();
        [self.jobs.len() + self.done.len(), self.jobs.capacity() + self.done.capacity(), self.retention_len, self.leases.len(), self.scheduled.len(), ready]
    }

    /// Répartition estimée de la mémoire des jobs terminés et de la rétention (diagnostic).
    pub fn mem_report(&self) -> MemReport {
        let slot = std::mem::size_of::<(Id, Done)>();
        MemReport {
            active_jobs: self.jobs.len(),
            active_capacity: self.jobs.capacity(),
            done_entries: self.done.len(),
            done_capacity: self.done.capacity(),
            done_entry_size: std::mem::size_of::<Done>(),
            done_slot_size: slot,
            done_table_bytes: self.done.table_bytes(),
            done_rehashes: self.done.stats().rehashes,
            done_rehash_max_us: self.done.stats().rehash_max_us,
            done_rehash_total_us: self.done.stats().rehash_total_us,
            done_max_probe: self.done.stats().max_probe,
            done_shrinks: self.done.stats().shrinks,
            done_shrink_max_us: self.done.stats().shrink_max_us,
            retention_slices: self.retention.len(),
            retention_entries: self.retention_len,
            retention_capacity: self.retention.values().map(|q| q.capacity()).sum(),
            retention_bytes: self.retention.values().map(|q| q.capacity() * 16).sum::<usize>() + self.retention.len() * (std::mem::size_of::<VecDeque<u128>>() + 8),
        }
    }

    pub fn next_deadline(&self) -> Option<u64> {
        [
            self.scheduled.peek().map(|Reverse((t, _, _))| *t),
            self.leases.peek().map(|Reverse((t, _, _))| *t),
            self.retention
                .first_key_value()
                .and_then(|(_, q)| q.front())
                .and_then(|id| self.done.get(&Id::new(*id)))
                .map(|d| d.purge_at),
            // La réduction de la table `done` avance d'une sous-table par tick : il faut se réveiller pour la finir.
            self.shrink_pending.then_some(self.last_tick + 1),
        ]
        .into_iter()
        .flatten()
        .min()
    }

    /// Vrai (et réinitialisé) si au moins un job est devenu disponible depuis le dernier appel.
    pub fn take_ready_flag(&mut self) -> bool {
        std::mem::take(&mut self.became_ready)
    }

    pub fn tick(&mut self, now: u64) {
        self.last_tick = now;
        while let Some(&Reverse((t, id, seq))) = self.scheduled.peek() {
            if t > now {
                break;
            }
            self.scheduled.pop();
            if self.jobs.get(&id).is_some_and(|j| j.state == JobState::Scheduled && j.seq == seq) {
                self.make_ready(id);
            }
        }
        while let Some(&Reverse((t, id, fence))) = self.leases.peek() {
            if t > now {
                break;
            }
            self.leases.pop();
            let valid = self.jobs.get(&id).is_some_and(|j| {
                j.state == JobState::Claimed && j.lease_deadline == t && j.fencing_token == fence
            });
            if valid {
                self.counters.lease_expirations += 1;
                self.retry_or_die(id, None, now);
            }
        }
        // Les tranches suivent l'ordre d'insertion : une échéance hors ordre peut retarder une purge
        // (d'au plus une tranche), jamais l'avancer.
        while let Some(mut slot) = self.retention.first_entry() {
            let q = slot.get_mut();
            while let Some(&id) = q.front() {
                if self.done.get(&Id::new(id)).is_some_and(|d| d.purge_at > now) {
                    break;
                }
                q.pop_front();
                self.retention_len -= 1;
                if let Some(d) = self.done.remove(&Id::new(id)) {
                    if d.created_off == WIDE {
                        self.wide.remove(&Id::new(id));
                    }
                    let c = &mut self.counts.entry(d.channel_id).or_insert([0; STATES])[d.state as usize];
                    *c = c.wrapping_add_signed(-1);
                }
            }
            if !q.is_empty() {
                break;
            }
            slot.remove();
        }
        if self.leases.len() > self.leases_compact_at {
            let jobs = &self.jobs;
            self.leases.retain(|Reverse((t, id, fence))| {
                jobs.get(id).is_some_and(|j| {
                    j.state == JobState::Claimed && j.lease_deadline == *t && j.fencing_token == *fence
                })
            });
            self.leases.shrink_to_fit();
            self.leases_compact_at = 2 * self.leases.len() + LEASES_COMPACT_MIN;
        }
        // La table ne rétrécit jamais seule : on la réduit quand l'écart de capacité devient grand
        // (rare, donc le coût du rehash est amorti).
        if self.jobs.capacity() > 4 * self.jobs.len() + 1024 {
            self.jobs.shrink_to(self.jobs.len() * 2 + 1024);
        }
        let was_pending = self.shrink_pending;
        self.shrink_pending = self.done.capacity() > 4 * self.done.len() + 1024 && self.done.shrink_to(self.done.len() * 2 + 1024);
        self.shrink_finished |= was_pending && !self.shrink_pending;
    }

    /// Vrai (et réinitialisé) quand la réduction de la table `done` vient de se terminer : bon moment pour rendre la mémoire libre à l'OS.
    pub fn take_shrink_finished(&mut self) -> bool {
        std::mem::take(&mut self.shrink_finished)
    }

    pub fn push(&mut self, req: PushRequest, now: u64) -> Result<PushOutcome> {
        self.tick(now);
        if req.payload.len() > self.cfg.limits.max_payload_bytes {
            return Err(EngineError::new(ErrorCode::PayloadTooLarge, "payload exceeds max_payload_bytes"));
        }
        let id = if req.job_id == 0 { self.generate_id(now) } else { req.job_id };
        if self.jobs.contains_key(&id) || self.done.contains_key(&Id::new(id)) {
            return Ok(PushOutcome { job_id: id, duplicate: true });
        }
        if self.jobs.len() + self.done.len() >= self.cfg.limits.max_jobs_per_shard {
            return Err(EngineError::new(ErrorCode::Overloaded, "shard job limit reached"));
        }
        let max_attempts =
            if req.max_attempts == 0 { self.cfg.jobs.default_max_attempts } else { req.max_attempts };
        self.counters.pushed += 1;
        self.commit(Record::Push {
            job_id: id,
            channel_id: req.channel_id,
            priority: req.priority,
            max_attempts,
            created_at: now,
            scheduled_at: now.saturating_add(req.delay_ms),
            payload: req.payload,
        });
        Ok(PushOutcome { job_id: id, duplicate: false })
    }

    pub fn claim(&mut self, channel_id: u16, lease_ms: u32, now: u64) -> Result<Option<ClaimedJob>> {
        self.tick(now);
        let lease_ms = if lease_ms == 0 { self.cfg.jobs.default_lease_ms } else { lease_ms }
            .min(self.cfg.jobs.max_lease_ms);
        let Some(id) = self.pop_ready(channel_id) else { return Ok(None) };

        let reservation_id = ((self.id as u64) << 48) | (self.next_reservation & RESERVATION_MASK);
        let fencing_token = self.jobs[&id].fencing_token + 1;
        let lease_deadline = now + lease_ms as u64;
        self.commit(Record::Claim { job_id: id, reservation_id, fencing_token, lease_deadline });
        let job = &self.jobs[&id];
        let claimed = ClaimedJob {
            job_id: id,
            reservation_id,
            fencing_token,
            lease_deadline,
            attempts: job.attempts,
            payload: job.payload.clone(),
        };
        self.counters.claims += 1;
        Ok(Some(claimed))
    }

    pub fn ack(&mut self, r: Reservation, now: u64) -> Result<()> {
        self.tick(now);
        self.validate(&r)?;
        self.commit(Record::Ack { job_id: r.job_id, at: now });
        self.counters.acks += 1;
        Ok(())
    }

    pub fn nack(&mut self, r: Reservation, kind: NackKind, now: u64) -> Result<()> {
        self.tick(now);
        self.validate(&r)?;
        self.counters.nacks += 1;
        match kind {
            NackKind::Fail => self.kill(r.job_id, now),
            NackKind::Retry { delay_ms } => self.retry_or_die(r.job_id, delay_ms, now),
        }
        Ok(())
    }

    /// Renouvelle le bail ; retourne la nouvelle échéance. Ne raccourcit jamais un bail.
    pub fn extend_lease(&mut self, r: Reservation, lease_ms: u32, now: u64) -> Result<u64> {
        self.tick(now);
        self.validate(&r)?;
        let lease_ms = if lease_ms == 0 { self.cfg.jobs.default_lease_ms } else { lease_ms }
            .min(self.cfg.jobs.max_lease_ms);
        let deadline = self.jobs[&r.job_id].lease_deadline.max(now + lease_ms as u64);
        self.commit(Record::Extend { job_id: r.job_id, lease_deadline: deadline });
        Ok(deadline)
    }

    pub fn get(&mut self, job_id: u128, now: u64) -> Result<JobView> {
        self.tick(now);
        if let Some(d) = self.done.get(&Id::new(job_id)) {
            let (created_at, scheduled_at) = self.done_times(Id::new(job_id), d);
            return Ok(JobView {
                job_id,
                channel_id: d.channel_id,
                state: d.state,
                priority: d.priority,
                attempts: d.attempts,
                max_attempts: d.max_attempts,
                fencing_token: d.fencing_token,
                lease_deadline: 0,
                scheduled_at,
                created_at,
                payload_length: d.payload_len,
            });
        }
        let j = self.jobs.get(&job_id).ok_or_else(not_found)?;
        Ok(JobView {
            job_id: j.id,
            channel_id: j.channel_id,
            state: j.state,
            priority: j.priority,
            attempts: j.attempts,
            max_attempts: j.max_attempts,
            fencing_token: j.fencing_token,
            lease_deadline: j.lease_deadline,
            scheduled_at: j.scheduled_at,
            created_at: j.created_at,
            payload_length: j.payload_len,
        })
    }

    pub fn cancel(&mut self, job_id: u128, now: u64) -> Result<()> {
        self.tick(now);
        let state = match self.jobs.get(&job_id) {
            Some(j) => j.state,
            None => self.done.get(&Id::new(job_id)).ok_or_else(not_found)?.state,
        };
        if !matches!(state, JobState::Ready | JobState::Scheduled) {
            return Err(EngineError::new(ErrorCode::InvalidState, format!("cannot cancel a {state:?} job")));
        }
        self.commit(Record::Cancel { job_id, at: now });
        Ok(())
    }

    pub fn stats(&mut self, channel_id: Option<u16>, now: u64) -> Stats {
        self.tick(now);
        let mut s = self.counters;
        let mut add = |c: &[u64; STATES]| {
            s.ready += c[JobState::Ready as usize];
            s.claimed += c[JobState::Claimed as usize];
            s.completed += c[JobState::Completed as usize];
            s.scheduled += c[JobState::Scheduled as usize];
            s.dead += c[JobState::Dead as usize];
            s.cancelled += c[JobState::Cancelled as usize];
        };
        match channel_id {
            Some(ch) => self.counts.get(&ch).into_iter().for_each(&mut add),
            None => self.counts.values().for_each(&mut add),
        }
        s
    }

    /// État complet du shard (jobs, réservations actives, fencing tokens, échéances).
    /// Les compteurs cumulatifs de `Stats` ne sont pas persistés.
    pub fn encode_snapshot(&self) -> Vec<u8> {
        let mut w = Wr::default();
        w.u64(self.next_reservation);
        w.u64(self.next_seq);
        w.u64((self.jobs.len() + self.done.len()) as u64);
        for (id, d) in self.done.iter() {
            let (created_at, scheduled_at) = self.done_times(id, d);
            w.u128(id.get());
            w.u16(d.channel_id);
            w.u8(d.state as u8);
            w.u8(d.priority);
            w.u32(d.attempts);
            w.u32(d.max_attempts);
            w.u64(d.fencing_token);
            w.u64(0);
            w.u64(0);
            w.u64(scheduled_at);
            w.u64(created_at);
            w.u64(0);
            w.u64(d.purge_at);
            w.u32(d.payload_len);
            w.bytes(&[]);
        }
        for j in self.jobs.values() {
            w.u128(j.id);
            w.u16(j.channel_id);
            w.u8(j.state as u8);
            w.u8(j.priority);
            w.u32(j.attempts);
            w.u32(j.max_attempts);
            w.u64(j.fencing_token);
            w.u64(j.reservation_id);
            w.u64(j.lease_deadline);
            w.u64(j.scheduled_at);
            w.u64(j.created_at);
            w.u64(j.seq);
            w.u64(0);
            w.u32(j.payload_len);
            w.bytes(&j.payload);
        }
        w.0
    }

    pub fn from_snapshot(id: u16, cfg: Arc<Config>, data: &[u8]) -> std::result::Result<Self, String> {
        let mut shard = Self::new(id, cfg);
        let mut r = Rd(data);
        shard.next_reservation = r.u64()?;
        shard.next_seq = r.u64()?;
        let n = r.u64()?;
        let mut jobs = Vec::new();
        for _ in 0..n {
            let j = Job {
                id: r.u128()?,
                channel_id: r.u16()?,
                state: crate::record::state_from(r.u8()?)?,
                priority: r.u8()?,
                attempts: r.u32()?,
                max_attempts: r.u32()?,
                fencing_token: r.u64()?,
                reservation_id: r.u64()?,
                lease_deadline: r.u64()?,
                scheduled_at: r.u64()?,
                created_at: r.u64()?,
                seq: r.u64()?,
                payload_len: 0,
                payload: Vec::new(),
            };
            let purge_at = r.u64()?;
            let payload_len = r.u32()?;
            let payload = r.bytes()?;
            jobs.push((Job { payload_len, payload, ..j }, purge_at));
        }
        r.finish()?;
        // L'ordre FIFO est restauré grâce au numéro de séquence.
        jobs.sort_by_key(|(j, _)| j.seq);
        let mut expiring = Vec::new();
        for (j, purge_at) in jobs {
            let (id, seq, ch, prio) = (j.id, j.seq, j.channel_id, j.priority);
            shard.bump(ch, j.state, 1);
            match j.state {
                JobState::Ready => {
                    shard.queues.entry(ch).or_default().ready.entry(prio).or_default().push_back((id, seq))
                }
                JobState::Scheduled => shard.scheduled.push(Reverse((j.scheduled_at, id, seq))),
                JobState::Claimed => shard.leases.push(Reverse((j.lease_deadline, id, j.fencing_token))),
                _ => {
                    shard.insert_done(id, &j, purge_at);
                    expiring.push((purge_at, id));
                    continue;
                }
            }
            shard.jobs.insert(id, j);
        }
        expiring.sort_unstable();
        for (purge_at, id) in expiring {
            shard.expire_at(id, purge_at);
        }
        Ok(shard)
    }

    fn validate(&self, r: &Reservation) -> Result<()> {
        let Some(j) = self.jobs.get(&r.job_id) else {
            return Err(if self.done.contains_key(&Id::new(r.job_id)) { stale() } else { not_found() });
        };
        if j.state != JobState::Claimed || j.reservation_id != r.reservation_id || j.fencing_token != r.fencing_token
        {
            return Err(stale());
        }
        Ok(())
    }

    fn retry_or_die(&mut self, id: u128, delay_ms: Option<u64>, now: u64) {
        let (attempts, max) = {
            let j = &self.jobs[&id];
            (j.attempts, j.max_attempts)
        };
        if attempts >= max {
            self.kill(id, now);
            return;
        }
        let delay = delay_ms.unwrap_or_else(|| self.backoff(attempts));
        self.commit(Record::Retry { job_id: id, at: now, scheduled_at: now.saturating_add(delay) });
    }

    fn kill(&mut self, id: u128, now: u64) {
        self.commit(Record::Dead { job_id: id, at: now });
    }

    fn insert_done(&mut self, id: u128, j: &Job, purge_at: u64) {
        let (d, wide) = Done::from_job(j, purge_at);
        if let Some(t) = wide {
            self.wide.insert(Id::new(id), t);
        }
        self.done.insert(Id::new(id), d);
    }

    fn done_times(&self, id: Id, d: &Done) -> (u64, u64) {
        if d.created_off == WIDE {
            return self.wide[&id];
        }
        (d.purge_at - d.created_off as u64, d.purge_at - d.scheduled_off as u64)
    }

    fn expire_at(&mut self, id: u128, purge_at: u64) {
        self.retention.entry(purge_at >> RETENTION_SLOT_SHIFT).or_default().push_back(id);
        self.retention_len += 1;
    }

    fn finish(&mut self, id: u128, purge_at: u64) {
        // Le job terminal quitte la table des jobs actifs : seule la trace (dédoublonnage, fencing) est conservée.
        let j = self.jobs.remove(&id).unwrap();
        self.insert_done(id, &j, purge_at);
        self.expire_at(id, purge_at);
    }

    /// `attempt` = nombre de tentatives déjà effectuées (>= 1).
    fn backoff(&mut self, attempt: u32) -> u64 {
        let r = &self.cfg.jobs.retry;
        let raw = match r.strategy {
            BackoffStrategy::Immediate => return 0,
            BackoffStrategy::Fixed => r.base_delay_ms,
            BackoffStrategy::Exponential => {
                r.base_delay_ms.saturating_mul(1u64 << (attempt.saturating_sub(1)).min(32))
            }
        };
        let delay = raw.min(r.max_delay_ms);
        let jitter = r.jitter;
        let frac = (self.next_rand() >> 11) as f64 / (1u64 << 53) as f64;
        (delay as f64 * (1.0 - jitter * frac)) as u64
    }

    fn schedule(&mut self, id: u128, at: u64) {
        let seq = self.bump_seq();
        self.set_state(id, JobState::Scheduled);
        self.jobs.get_mut(&id).unwrap().seq = seq;
        self.scheduled.push(Reverse((at, id, seq)));
    }

    fn make_ready(&mut self, id: u128) {
        let seq = self.bump_seq();
        self.set_state(id, JobState::Ready);
        let job = self.jobs.get_mut(&id).unwrap();
        job.seq = seq;
        let (ch, prio) = (job.channel_id, job.priority);
        self.queues.entry(ch).or_default().ready.entry(prio).or_default().push_back((id, seq));
        self.became_ready = true;
    }

    fn pop_ready(&mut self, channel_id: u16) -> Option<u128> {
        let queue = self.queues.get_mut(&channel_id)?;
        loop {
            let mut entry = queue.ready.last_entry()?;
            let Some((id, seq)) = entry.get_mut().pop_front() else {
                entry.remove();
                continue;
            };
            if entry.get().is_empty() {
                entry.remove();
            }
            if self.jobs.get(&id).is_some_and(|j| j.state == JobState::Ready && j.seq == seq) {
                return Some(id);
            }
        }
    }

    fn set_state(&mut self, id: u128, new: JobState) {
        let job = self.jobs.get_mut(&id).unwrap();
        let (ch, old) = (job.channel_id, job.state);
        job.state = new;
        self.bump(ch, old, -1);
        self.bump(ch, new, 1);
    }

    fn bump(&mut self, ch: u16, state: JobState, delta: i64) {
        let c = &mut self.counts.entry(ch).or_insert([0; STATES])[state as usize];
        *c = c.wrapping_add_signed(delta);
    }

    fn bump_seq(&mut self) -> u64 {
        self.next_seq += 1;
        self.next_seq
    }

    fn next_rand(&mut self) -> u64 {
        let mut x = self.rng;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng = x;
        x
    }

    fn generate_id(&mut self, now: u64) -> u128 {
        loop {
            let id = ((self.next_rand() as u128) << 64) | (self.next_rand() ^ now) as u128;
            if id != 0 && !self.jobs.contains_key(&id) {
                return id;
            }
        }
    }
}

fn stale() -> EngineError {
    EngineError::new(ErrorCode::StaleReservation, "reservation is no longer active")
}

fn not_found() -> EngineError {
    EngineError::new(ErrorCode::NotFound, "job not found")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::BackoffStrategy;

    fn shard_with(f: impl FnOnce(&mut Config)) -> Shard {
        let mut cfg = Config::default();
        cfg.jobs.retry.jitter = 0.0;
        f(&mut cfg);
        Shard::new(0, Arc::new(cfg))
    }

    fn push(id: u128, prio: u8, delay: u64) -> PushRequest {
        PushRequest {
            job_id: id,
            channel_id: 1,
            priority: prio,
            max_attempts: 0,
            delay_ms: delay,
            partition_key: vec![],
            payload: id.to_le_bytes().to_vec(),
        }
    }

    fn res(c: &ClaimedJob) -> Reservation {
        Reservation { job_id: c.job_id, reservation_id: c.reservation_id, fencing_token: c.fencing_token }
    }

    #[test]
    fn fifo_with_priorities() {
        let mut s = shard_with(|_| {});
        for (id, p) in [(1, 0), (2, 0), (3, 9)] {
            s.push(push(id, p, 0), 0).unwrap();
        }
        let order: Vec<u128> = (0..3).map(|_| s.claim(1, 0, 0).unwrap().unwrap().job_id).collect();
        assert_eq!(order, vec![3, 1, 2]);
        assert!(s.claim(1, 0, 0).unwrap().is_none());
    }

    #[test]
    fn push_is_idempotent() {
        let mut s = shard_with(|_| {});
        assert!(!s.push(push(7, 0, 0), 0).unwrap().duplicate);
        assert!(s.push(push(7, 0, 0), 0).unwrap().duplicate);
        assert_eq!(s.stats(Some(1), 0).ready, 1);
    }

    #[test]
    fn stale_fencing_token_is_rejected_after_expiry() {
        let mut s = shard_with(|c| c.jobs.retry.strategy = BackoffStrategy::Immediate);
        s.push(push(1, 0, 0), 0).unwrap();
        let old = s.claim(1, 1000, 0).unwrap().unwrap();
        // Le bail expire : le job est ré-attribué avec un token supérieur.
        let new = s.claim(1, 1000, 1000).unwrap().unwrap();
        assert!(new.fencing_token > old.fencing_token);
        assert_eq!(s.ack(res(&old), 1001).unwrap_err().code, ErrorCode::StaleReservation);
        s.ack(res(&new), 1001).unwrap();
        assert_eq!(s.get(1, 1001).unwrap().state, JobState::Completed);
        assert_eq!(s.stats(None, 1001).lease_expirations, 1);
    }

    #[test]
    fn extend_lease_prevents_expiry() {
        let mut s = shard_with(|_| {});
        s.push(push(1, 0, 0), 0).unwrap();
        let c = s.claim(1, 1000, 0).unwrap().unwrap();
        assert_eq!(s.extend_lease(res(&c), 5000, 900).unwrap(), 5900);
        assert_eq!(s.get(1, 2000).unwrap().state, JobState::Claimed);
        s.ack(res(&c), 2000).unwrap();
    }

    #[test]
    fn scheduled_job_becomes_ready_at_deadline() {
        let mut s = shard_with(|_| {});
        s.push(push(1, 0, 500), 0).unwrap();
        assert_eq!(s.next_deadline(), Some(500));
        assert!(s.claim(1, 0, 499).unwrap().is_none());
        assert!(s.claim(1, 0, 500).unwrap().is_some());
    }

    #[test]
    fn nack_applies_exponential_backoff_then_dies() {
        let mut s = shard_with(|c| {
            c.jobs.retry.base_delay_ms = 100;
            c.jobs.default_max_attempts = 3;
        });
        s.push(push(1, 0, 0), 0).unwrap();
        let c = s.claim(1, 0, 0).unwrap().unwrap();
        s.nack(res(&c), NackKind::Retry { delay_ms: None }, 0).unwrap();
        assert_eq!(s.get(1, 0).unwrap().scheduled_at, 100);
        assert!(s.claim(1, 0, 99).unwrap().is_none());
        let c = s.claim(1, 0, 100).unwrap().unwrap();
        s.nack(res(&c), NackKind::Retry { delay_ms: None }, 100).unwrap();
        assert_eq!(s.get(1, 100).unwrap().scheduled_at, 300);
        let c = s.claim(1, 0, 300).unwrap().unwrap();
        s.nack(res(&c), NackKind::Retry { delay_ms: None }, 300).unwrap();
        assert_eq!(s.get(1, 300).unwrap().state, JobState::Dead);
        assert_eq!(s.stats(None, 300).dead, 1);
    }

    #[test]
    fn cancel_only_before_claim() {
        let mut s = shard_with(|_| {});
        s.push(push(1, 0, 0), 0).unwrap();
        s.push(push(2, 0, 0), 0).unwrap();
        s.cancel(1, 0).unwrap();
        let c = s.claim(1, 0, 0).unwrap().unwrap();
        assert_eq!(c.job_id, 2);
        assert_eq!(s.cancel(2, 0).unwrap_err().code, ErrorCode::InvalidState);
        assert!(s.claim(1, 0, 0).unwrap().is_none());
    }

    #[test]
    fn limits_and_retention() {
        let mut s = shard_with(|c| {
            c.limits.max_jobs_per_shard = 1;
            c.limits.max_payload_bytes = 4;
            c.jobs.completed_retention_ms = 10;
        });
        let mut big = push(1, 0, 0);
        big.payload = vec![0; 5];
        assert_eq!(s.push(big, 0).unwrap_err().code, ErrorCode::PayloadTooLarge);
        let mut small = push(1, 0, 0);
        small.payload = vec![];
        s.push(small, 0).unwrap();
        let mut second = push(2, 0, 0);
        second.payload = vec![];
        assert_eq!(s.push(second, 0).unwrap_err().code, ErrorCode::Overloaded);
        let c = s.claim(1, 0, 0).unwrap().unwrap();
        s.ack(res(&c), 0).unwrap();
        assert_eq!(s.get(1, 9).unwrap().state, JobState::Completed);
        assert_eq!(s.get(1, 10).unwrap_err().code, ErrorCode::NotFound);
    }

    /// Après une grosse purge, la table `done` finit de rétrécir sans activité : `next_deadline` réveille le shard
    /// jusqu'à ce que `tick` n'ait plus rien à réduire.
    #[test]
    fn done_table_shrinks_completely_after_large_purge() {
        let mut s = shard_with(|c| c.jobs.completed_retention_ms = 1000);
        for id in 1..=100_000u128 {
            s.push(push(id, 0, 0), 0).unwrap();
            let c = s.claim(1, 0, 0).unwrap().unwrap();
            s.ack(res(&c), 0).unwrap();
        }
        let full = s.done.capacity();
        assert!(full >= 100_000);
        let mut now = 1000;
        let mut wakes = 0;
        s.tick(now);
        while s.shrink_pending {
            assert_eq!(s.next_deadline(), Some(now + 1));
            now += 1;
            s.tick(now);
            wakes += 1;
            assert!(wakes < 1000, "la réduction ne converge pas");
        }
        assert!(s.take_shrink_finished());
        assert!(!s.take_shrink_finished());
        assert_eq!(s.done.len(), 0);
        assert!(s.done.capacity() <= 4 * 1024, "capacité résiduelle {} (avant : {full})", s.done.capacity());
    }

    #[test]
    fn restored_terminal_job_expires_at_its_exact_deadline() {
        let mut s = shard_with(|c| c.jobs.completed_retention_ms = 1000);
        for id in 1..=3 {
            s.push(push(id, 0, 0), 100 * id as u64).unwrap();
            let c = s.claim(1, 0, 100 * id as u64).unwrap().unwrap();
            s.ack(res(&c), 100 * id as u64).unwrap();
        }
        let mut r = Shard::from_snapshot(0, s.cfg.clone(), &s.encode_snapshot()).unwrap();
        for id in 1..=3u128 {
            let due = 100 * id as u64 + 1000;
            assert_eq!(r.get(id, due - 1).unwrap().state, JobState::Completed, "job {id} purgé trop tôt");
            if id == 1 {
                assert_eq!(r.get(1, due).unwrap_err().code, ErrorCode::NotFound);
            }
        }
        assert_eq!(r.get(2, 1200).unwrap_err().code, ErrorCode::NotFound);
        assert_eq!(r.get(3, 1300).unwrap_err().code, ErrorCode::NotFound);
    }

    #[test]
    fn terminal_job_keeps_payload_length_but_not_payload() {
        let mut s = shard_with(|_| {});
        s.push(push(1, 0, 0), 0).unwrap();
        let len = s.get(1, 0).unwrap().payload_length;
        assert!(len > 0);
        let c = s.claim(1, 0, 0).unwrap().unwrap();
        s.ack(res(&c), 0).unwrap();
        assert_eq!(s.get(1, 1).unwrap().payload_length, len);
        assert!(s.jobs.get(&1).is_none());
        let restored = Shard::from_snapshot(0, s.cfg.clone(), &s.encode_snapshot()).unwrap();
        assert_eq!(restored.done.get(&Id::new(1)).unwrap().payload_len, len);
    }

    #[test]
    fn done_slot_is_56_bytes() {
        assert_eq!(std::mem::size_of::<(Id, Done)>(), 56);
    }

    #[test]
    fn offsets_switch_to_wide_exactly_at_u32_max() {
        assert_eq!(offset_below(10_000_000_000, 10_000_000_000 - (WIDE as u64 - 1)), Some(WIDE - 1));
        assert_eq!(offset_below(10_000_000_000, 10_000_000_000 - WIDE as u64), None);
        assert_eq!(offset_below(5, 6), None);
        assert_eq!(offset_below(5, 5), Some(0));
    }

    #[test]
    fn schedule_after_purge_deadline_stays_exact_through_snapshot_and_purge() {
        let mut s = shard_with(|c| c.jobs.completed_retention_ms = 1000);
        // Annulé avant son échéance, qui tombe après `purge_at`.
        s.push(push(2, 0, 10_000_000), 5).unwrap();
        s.cancel(2, 5).unwrap();
        assert_eq!(s.wide.len(), 1);
        let mut r = Shard::from_snapshot(0, s.cfg.clone(), &s.encode_snapshot()).unwrap();
        for sh in [&mut s, &mut r] {
            let v = sh.get(2, 6).unwrap();
            assert_eq!((v.created_at, v.scheduled_at), (5, 10_000_005));
        }
        assert_eq!(r.wide.len(), 1);
        assert_eq!(r.get(2, 1005).unwrap_err().code, ErrorCode::NotFound);
        assert!(r.wide.is_empty());
    }

    #[test]
    fn job_completed_more_than_u32_ms_after_creation_stays_exact() {
        let mut s = shard_with(|c| c.jobs.completed_retention_ms = 1000);
        let late = 60 * 24 * 3600 * 1000u64;
        s.push(push(1, 0, 0), 0).unwrap();
        let c = s.claim(1, 0, late).unwrap().unwrap();
        s.ack(res(&c), late).unwrap();
        assert_eq!(s.wide.len(), 1);
        let mut r = Shard::from_snapshot(0, s.cfg.clone(), &s.encode_snapshot()).unwrap();
        for sh in [&mut s, &mut r] {
            let v = sh.get(1, late + 1).unwrap();
            assert_eq!((v.created_at, v.scheduled_at), (0, 0));
        }
        assert_eq!(r.get(1, late + 1000).unwrap_err().code, ErrorCode::NotFound);
        assert!(r.wide.is_empty());
    }
}
