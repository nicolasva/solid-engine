//! Client bloquant de référence (une requête à la fois par connexion).

use crate::{read_frame, write_frame, ProtocolError, Request, Response};
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::os::unix::net::UnixStream;
use std::path::Path;

const MAX_RESPONSE_BYTES: usize = 64 << 20;

pub struct Client<S: Read + Write> {
    stream: S,
    next_id: u64,
}

impl Client<TcpStream> {
    pub fn connect_tcp(addr: impl ToSocketAddrs) -> std::io::Result<Self> {
        let s = TcpStream::connect(addr)?;
        s.set_nodelay(true)?;
        Ok(Self::new(s))
    }
}

impl Client<UnixStream> {
    pub fn connect_unix(path: impl AsRef<Path>) -> std::io::Result<Self> {
        Ok(Self::new(UnixStream::connect(path)?))
    }
}

impl<S: Read + Write> Client<S> {
    pub fn new(stream: S) -> Self {
        Self { stream, next_id: 1 }
    }

    pub fn call(&mut self, req: &Request) -> Result<Response, ProtocolError> {
        let id = self.next_id;
        self.next_id += 1;
        write_frame(&mut self.stream, &req.to_frame(id))?;
        let frame = read_frame(&mut self.stream, MAX_RESPONSE_BYTES)?;
        if frame.request_id != id {
            return Err(ProtocolError::Malformed("response for unexpected request id"));
        }
        Response::decode(frame.msg_type, &frame.body)
    }
}
