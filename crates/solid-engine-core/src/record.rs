//! Enregistrements WAL logiques (redo). Un enregistrement rejoué via `Shard::apply`
//! produit exactement la même mutation que l'opération live qui l'a émis.

use crate::JobState;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Record {
    Push {
        job_id: u128,
        channel_id: u16,
        priority: u8,
        max_attempts: u32,
        created_at: u64,
        scheduled_at: u64,
        payload: Vec<u8>,
    },
    Claim { job_id: u128, reservation_id: u64, fencing_token: u64, lease_deadline: u64 },
    Extend { job_id: u128, lease_deadline: u64 },
    Ack { job_id: u128, at: u64 },
    Retry { job_id: u128, at: u64, scheduled_at: u64 },
    Dead { job_id: u128, at: u64 },
    Cancel { job_id: u128, at: u64 },
}

pub const RECORD_VERSION: u8 = 1;

impl Record {
    pub fn job_id(&self) -> u128 {
        match self {
            Self::Push { job_id, .. }
            | Self::Claim { job_id, .. }
            | Self::Extend { job_id, .. }
            | Self::Ack { job_id, .. }
            | Self::Retry { job_id, .. }
            | Self::Dead { job_id, .. }
            | Self::Cancel { job_id, .. } => *job_id,
        }
    }

    pub fn record_type(&self) -> u8 {
        match self {
            Self::Push { .. } => 1,
            Self::Claim { .. } => 2,
            Self::Extend { .. } => 3,
            Self::Ack { .. } => 4,
            Self::Retry { .. } => 5,
            Self::Dead { .. } => 6,
            Self::Cancel { .. } => 7,
        }
    }

    pub fn encode_payload(&self) -> Vec<u8> {
        let mut w = Wr::default();
        match self {
            Self::Push { channel_id, priority, max_attempts, created_at, scheduled_at, payload, .. } => {
                w.u16(*channel_id);
                w.u8(*priority);
                w.u32(*max_attempts);
                w.u64(*created_at);
                w.u64(*scheduled_at);
                w.bytes(payload);
            }
            Self::Claim { reservation_id, fencing_token, lease_deadline, .. } => {
                w.u64(*reservation_id);
                w.u64(*fencing_token);
                w.u64(*lease_deadline);
            }
            Self::Extend { lease_deadline, .. } => w.u64(*lease_deadline),
            Self::Ack { at, .. } | Self::Dead { at, .. } | Self::Cancel { at, .. } => w.u64(*at),
            Self::Retry { at, scheduled_at, .. } => {
                w.u64(*at);
                w.u64(*scheduled_at);
            }
        }
        w.0
    }

    pub fn decode(record_type: u8, job_id: u128, payload: &[u8]) -> Result<Self, String> {
        let mut r = Rd(payload);
        let rec = match record_type {
            1 => Self::Push {
                job_id,
                channel_id: r.u16()?,
                priority: r.u8()?,
                max_attempts: r.u32()?,
                created_at: r.u64()?,
                scheduled_at: r.u64()?,
                payload: r.bytes()?,
            },
            2 => Self::Claim {
                job_id,
                reservation_id: r.u64()?,
                fencing_token: r.u64()?,
                lease_deadline: r.u64()?,
            },
            3 => Self::Extend { job_id, lease_deadline: r.u64()? },
            4 => Self::Ack { job_id, at: r.u64()? },
            5 => Self::Retry { job_id, at: r.u64()?, scheduled_at: r.u64()? },
            6 => Self::Dead { job_id, at: r.u64()? },
            7 => Self::Cancel { job_id, at: r.u64()? },
            t => return Err(format!("unknown record type {t}")),
        };
        r.finish()?;
        Ok(rec)
    }
}

#[derive(Default)]
pub(crate) struct Wr(pub Vec<u8>);

impl Wr {
    pub fn u8(&mut self, v: u8) {
        self.0.push(v);
    }
    pub fn u16(&mut self, v: u16) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    pub fn u32(&mut self, v: u32) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    pub fn u64(&mut self, v: u64) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    pub fn u128(&mut self, v: u128) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    pub fn bytes(&mut self, b: &[u8]) {
        self.u32(b.len() as u32);
        self.0.extend_from_slice(b);
    }
}

pub(crate) struct Rd<'a>(pub &'a [u8]);

impl<'a> Rd<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        if self.0.len() < n {
            return Err("truncated data".into());
        }
        let (a, b) = self.0.split_at(n);
        self.0 = b;
        Ok(a)
    }
    pub fn u8(&mut self) -> Result<u8, String> {
        Ok(self.take(1)?[0])
    }
    pub fn u16(&mut self) -> Result<u16, String> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    pub fn u32(&mut self) -> Result<u32, String> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    pub fn u64(&mut self) -> Result<u64, String> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub fn u128(&mut self) -> Result<u128, String> {
        Ok(u128::from_le_bytes(self.take(16)?.try_into().unwrap()))
    }
    pub fn bytes(&mut self) -> Result<Vec<u8>, String> {
        let n = self.u32()? as usize;
        Ok(self.take(n)?.to_vec())
    }
    pub fn finish(self) -> Result<(), String> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err("trailing bytes".into())
        }
    }
}

pub(crate) fn state_from(v: u8) -> Result<JobState, String> {
    JobState::from_u8(v).ok_or_else(|| format!("invalid job state {v}"))
}
