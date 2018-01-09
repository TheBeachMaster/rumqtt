use std::net::SocketAddr;
use std::io::{self, Read, Write, ErrorKind};

use tokio_core::net::TcpStream;
use tokio_core::reactor::Core;
use native_tls::TlsConnector;
use native_tls::Certificate;
use tokio_tls::{TlsConnectorExt, TlsStream};
use tokio_io::{AsyncRead, AsyncWrite};
use futures::{Future, Poll};

use error::ConnectError;

pub enum NetworkStream {
    Tcp(TcpStream),
    Tls(TlsStream<TcpStream>)
}

impl NetworkStream {
    pub fn new(addr: &SocketAddr, reactor: &mut Core) -> Result<NetworkStream, ConnectError> {
        let tcp = TcpStream::connect(&addr, &reactor.handle());
        let tcp = reactor.run(tcp)?;
        Ok(NetworkStream::Tcp(tcp))
    }

    pub fn tls(self, ca: Certificate, domain: &str, reactor: &mut Core) -> Result<NetworkStream, ConnectError> {
        let mut cx = TlsConnector::builder()?;
        cx.add_root_certificate(ca)?;
        let cx = cx.build()?;

        let tls = match self {
            NetworkStream::Tcp(tcp) => {
                let tls = cx.connect_async(domain, tcp);
                reactor.run(tls)?
            }
            NetworkStream::Tls(_tls) => return Err(io::Error::new(ErrorKind::AlreadyExists, "This is already a tls stream").into()),
        };

        Ok(NetworkStream::Tls(tls))
    }
}

impl Read for NetworkStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match *self {
            NetworkStream::Tcp(ref mut s) => s.read(buf),
            NetworkStream::Tls(ref mut s) => s.read(buf),
        }
    }
}

impl Write for NetworkStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match *self {
            NetworkStream::Tcp(ref mut s) => s.write(buf),
            NetworkStream::Tls(ref mut s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match *self {
            NetworkStream::Tcp(ref mut s) => s.flush(),
            NetworkStream::Tls(ref mut s) => s.flush(),
        }
    }
}

impl AsyncRead for NetworkStream{}
impl AsyncWrite for NetworkStream{
    fn shutdown(&mut self) -> Poll<(), io::Error> {
        match *self {
            NetworkStream::Tcp(ref mut s) => s.shutdown(),
            NetworkStream::Tls(ref mut s) => s.shutdown(),
        }
    }
}