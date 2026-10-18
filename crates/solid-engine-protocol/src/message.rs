use crate::codec::{Frame, ProtocolError, PutExt, Reader};
use crate::msg;
use solid_engine_core::{
    ClaimedJob, EngineError, ErrorCode, JobState, JobView, NackKind, PushOutcome, PushRequest, Reservation, Stats,
};

/// `delay_ms` de NACK : valeur sentinelle pour « appliquer la politique de backoff ».
const DELAY_FROM_POLICY: u64 = u64::MAX;
const NACK_FAIL: u8 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Request {
    Ping,
    PushJob(PushRequest),
    ClaimNextJob { channel_id: u16, lease_ms: u32, wait_ms: u32 },
    AckJob(Reservation),
    NackJob { reservation: Reservation, kind: NackKind },
    ExtendLease { reservation: Reservation, lease_ms: u32 },
    GetJob { job_id: u128 },
    CancelJob { job_id: u128 },
    WaitForJob { channel_id: u16, timeout_ms: u32 },
    GetQueueStats { channel_id: Option<u16> },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Response {
    Pong,
    Push(PushOutcome),
    Claim(Option<ClaimedJob>),
    Ack,
    Nack,
    ExtendLease { lease_deadline: u64 },
    Job(JobView),
    Cancel,
    Wait { available: bool },
    Stats(Stats),
    Error(EngineError),
}

fn put_reservation(b: &mut Vec<u8>, r: &Reservation) {
    b.put_u128(r.job_id);
    b.put_u64(r.reservation_id);
    b.put_u64(r.fencing_token);
}

fn get_reservation(r: &mut Reader) -> Result<Reservation, ProtocolError> {
    Ok(Reservation { job_id: r.u128()?, reservation_id: r.u64()?, fencing_token: r.u64()? })
}

impl Request {
    pub fn msg_type(&self) -> u8 {
        match self {
            Self::Ping => msg::PING,
            Self::PushJob(_) => msg::PUSH_JOB,
            Self::ClaimNextJob { .. } => msg::CLAIM_NEXT_JOB,
            Self::AckJob(_) => msg::ACK_JOB,
            Self::NackJob { .. } => msg::NACK_JOB,
            Self::ExtendLease { .. } => msg::EXTEND_LEASE,
            Self::GetJob { .. } => msg::GET_JOB,
            Self::CancelJob { .. } => msg::CANCEL_JOB,
            Self::WaitForJob { .. } => msg::WAIT_FOR_JOB,
            Self::GetQueueStats { .. } => msg::GET_QUEUE_STATS,
        }
    }

    pub fn encode_body(&self) -> Vec<u8> {
        let mut b = Vec::new();
        match self {
            Self::Ping => {}
            Self::PushJob(p) => {
                b.put_u128(p.job_id);
                b.put_u16(p.channel_id);
                b.push(p.priority);
                b.put_u32(p.max_attempts);
                b.put_u64(p.delay_ms);
                b.put_bytes(&p.partition_key);
                b.put_bytes(&p.payload);
            }
            Self::ClaimNextJob { channel_id, lease_ms, wait_ms } => {
                b.put_u16(*channel_id);
                b.put_u32(*lease_ms);
                b.put_u32(*wait_ms);
            }
            Self::AckJob(r) => put_reservation(&mut b, r),
            Self::NackJob { reservation, kind } => {
                put_reservation(&mut b, reservation);
                match kind {
                    NackKind::Fail => {
                        b.push(NACK_FAIL);
                        b.put_u64(0);
                    }
                    NackKind::Retry { delay_ms } => {
                        b.push(0);
                        b.put_u64(delay_ms.unwrap_or(DELAY_FROM_POLICY));
                    }
                }
            }
            Self::ExtendLease { reservation, lease_ms } => {
                put_reservation(&mut b, reservation);
                b.put_u32(*lease_ms);
            }
            Self::GetJob { job_id } | Self::CancelJob { job_id } => b.put_u128(*job_id),
            Self::WaitForJob { channel_id, timeout_ms } => {
                b.put_u16(*channel_id);
                b.put_u32(*timeout_ms);
            }
            Self::GetQueueStats { channel_id } => {
                b.push(channel_id.is_some() as u8);
                b.put_u16(channel_id.unwrap_or(0));
            }
        }
        b
    }

    pub fn decode(msg_type: u8, body: &[u8]) -> Result<Self, ProtocolError> {
        let mut r = Reader::new(body);
        let req = match msg_type {
            msg::PING => Self::Ping,
            msg::PUSH_JOB => Self::PushJob(PushRequest {
                job_id: r.u128()?,
                channel_id: r.u16()?,
                priority: r.u8()?,
                max_attempts: r.u32()?,
                delay_ms: r.u64()?,
                partition_key: r.bytes()?,
                payload: r.bytes()?,
            }),
            msg::CLAIM_NEXT_JOB => {
                Self::ClaimNextJob { channel_id: r.u16()?, lease_ms: r.u32()?, wait_ms: r.u32()? }
            }
            msg::ACK_JOB => Self::AckJob(get_reservation(&mut r)?),
            msg::NACK_JOB => {
                let reservation = get_reservation(&mut r)?;
                let (flags, delay) = (r.u8()?, r.u64()?);
                let kind = if flags & NACK_FAIL != 0 {
                    NackKind::Fail
                } else {
                    NackKind::Retry { delay_ms: (delay != DELAY_FROM_POLICY).then_some(delay) }
                };
                Self::NackJob { reservation, kind }
            }
            msg::EXTEND_LEASE => {
                Self::ExtendLease { reservation: get_reservation(&mut r)?, lease_ms: r.u32()? }
            }
            msg::GET_JOB => Self::GetJob { job_id: r.u128()? },
            msg::CANCEL_JOB => Self::CancelJob { job_id: r.u128()? },
            msg::WAIT_FOR_JOB => Self::WaitForJob { channel_id: r.u16()?, timeout_ms: r.u32()? },
            msg::GET_QUEUE_STATS => {
                let (has, ch) = (r.u8()?, r.u16()?);
                Self::GetQueueStats { channel_id: (has != 0).then_some(ch) }
            }
            _ => return Err(ProtocolError::Malformed("unknown message type")),
        };
        r.finish()?;
        Ok(req)
    }

    pub fn to_frame(&self, request_id: u64) -> Frame {
        Frame { msg_type: self.msg_type(), flags: 0, request_id, body: self.encode_body() }
    }
}

impl Response {
    pub fn msg_type(&self, request_type: u8) -> u8 {
        match self {
            Self::Error(_) => msg::ERROR,
            _ => request_type | msg::RESPONSE_BIT,
        }
    }

    pub fn encode_body(&self) -> Vec<u8> {
        let mut b = Vec::new();
        match self {
            Self::Pong | Self::Ack | Self::Nack | Self::Cancel => {}
            Self::Push(o) => {
                b.put_u128(o.job_id);
                b.push(o.duplicate as u8);
            }
            Self::Claim(None) => b.push(0),
            Self::Claim(Some(c)) => {
                b.push(1);
                b.put_u128(c.job_id);
                b.put_u64(c.reservation_id);
                b.put_u64(c.fencing_token);
                b.put_u64(c.lease_deadline);
                b.put_u32(c.attempts);
                b.put_bytes(&c.payload);
            }
            Self::ExtendLease { lease_deadline } => b.put_u64(*lease_deadline),
            Self::Job(j) => {
                b.put_u128(j.job_id);
                b.put_u16(j.channel_id);
                b.push(j.state as u8);
                b.push(j.priority);
                b.put_u32(j.attempts);
                b.put_u32(j.max_attempts);
                b.put_u64(j.fencing_token);
                b.put_u64(j.lease_deadline);
                b.put_u64(j.scheduled_at);
                b.put_u64(j.created_at);
                b.put_u32(j.payload_length);
            }
            Self::Wait { available } => b.push(*available as u8),
            Self::Stats(s) => {
                for v in [
                    s.ready, s.claimed, s.scheduled, s.completed, s.dead, s.cancelled, s.pushed, s.claims,
                    s.acks, s.nacks, s.lease_expirations,
                ] {
                    b.put_u64(v);
                }
            }
            Self::Error(e) => {
                b.put_u16(e.code as u16);
                b.put_bytes(e.message.as_bytes());
            }
        }
        b
    }

    pub fn to_frame(&self, request_type: u8, request_id: u64) -> Frame {
        Frame { msg_type: self.msg_type(request_type), flags: 0, request_id, body: self.encode_body() }
    }

    pub fn decode(msg_type: u8, body: &[u8]) -> Result<Self, ProtocolError> {
        let mut r = Reader::new(body);
        let resp = if msg_type == msg::ERROR {
            let code = ErrorCode::from_u16(r.u16()?);
            let message = String::from_utf8_lossy(&r.bytes()?).into_owned();
            Self::Error(EngineError { code, message })
        } else {
            match msg_type & !msg::RESPONSE_BIT {
                msg::PING => Self::Pong,
                msg::PUSH_JOB => Self::Push(PushOutcome { job_id: r.u128()?, duplicate: r.u8()? != 0 }),
                msg::CLAIM_NEXT_JOB => {
                    if r.u8()? == 0 {
                        Self::Claim(None)
                    } else {
                        Self::Claim(Some(ClaimedJob {
                            job_id: r.u128()?,
                            reservation_id: r.u64()?,
                            fencing_token: r.u64()?,
                            lease_deadline: r.u64()?,
                            attempts: r.u32()?,
                            payload: r.bytes()?,
                        }))
                    }
                }
                msg::ACK_JOB => Self::Ack,
                msg::NACK_JOB => Self::Nack,
                msg::EXTEND_LEASE => Self::ExtendLease { lease_deadline: r.u64()? },
                msg::GET_JOB => Self::Job(JobView {
                    job_id: r.u128()?,
                    channel_id: r.u16()?,
                    state: JobState::from_u8(r.u8()?).ok_or(ProtocolError::Malformed("bad job state"))?,
                    priority: r.u8()?,
                    attempts: r.u32()?,
                    max_attempts: r.u32()?,
                    fencing_token: r.u64()?,
                    lease_deadline: r.u64()?,
                    scheduled_at: r.u64()?,
                    created_at: r.u64()?,
                    payload_length: r.u32()?,
                }),
                msg::CANCEL_JOB => Self::Cancel,
                msg::WAIT_FOR_JOB => Self::Wait { available: r.u8()? != 0 },
                msg::GET_QUEUE_STATS => Self::Stats(Stats {
                    ready: r.u64()?,
                    claimed: r.u64()?,
                    scheduled: r.u64()?,
                    completed: r.u64()?,
                    dead: r.u64()?,
                    cancelled: r.u64()?,
                    pushed: r.u64()?,
                    claims: r.u64()?,
                    acks: r.u64()?,
                    nacks: r.u64()?,
                    lease_expirations: r.u64()?,
                }),
                _ => return Err(ProtocolError::Malformed("unknown response type")),
            }
        };
        r.finish()?;
        Ok(resp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{read_frame, write_frame};

    #[test]
    fn request_roundtrip_through_frame() {
        let req = Request::PushJob(PushRequest {
            job_id: 42,
            channel_id: 3,
            priority: 5,
            max_attempts: 2,
            delay_ms: 10,
            partition_key: b"k".to_vec(),
            payload: vec![1, 2, 3],
        });
        let mut wire = Vec::new();
        write_frame(&mut wire, &req.to_frame(9)).unwrap();
        let f = read_frame(&mut wire.as_slice(), 1024).unwrap();
        assert_eq!(f.request_id, 9);
        assert_eq!(Request::decode(f.msg_type, &f.body).unwrap(), req);
    }

    #[test]
    fn corrupted_frame_and_oversize_are_rejected() {
        let mut wire = Vec::new();
        write_frame(&mut wire, &Request::Ping.to_frame(1)).unwrap();
        let mut bad = wire.clone();
        bad[9] ^= 0xFF;
        assert!(matches!(read_frame(&mut bad.as_slice(), 1024), Err(ProtocolError::ChecksumMismatch)));

        let big = Request::GetJob { job_id: 1 }.to_frame(1);
        let mut wire = Vec::new();
        write_frame(&mut wire, &big).unwrap();
        assert!(matches!(read_frame(&mut wire.as_slice(), 4), Err(ProtocolError::TooLarge(16))));
    }

    #[test]
    fn response_roundtrip() {
        let resp = Response::Error(EngineError::new(ErrorCode::StaleReservation, "nope"));
        let f = resp.to_frame(msg::ACK_JOB, 1);
        assert_eq!(Response::decode(f.msg_type, &f.body).unwrap(), resp);
        let resp = Response::Stats(Stats { ready: 1, acks: 4, ..Default::default() });
        let f = resp.to_frame(msg::GET_QUEUE_STATS, 1);
        assert_eq!(Response::decode(f.msg_type, &f.body).unwrap(), resp);
    }
}
