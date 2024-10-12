use std::io;

use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite};

pub struct MemoryNoOpRWS {
}
impl MemoryNoOpRWS {
    pub fn new() -> Self {
        Self {  }
    }
}

impl AsyncRead for MemoryNoOpRWS {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        _buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for MemoryNoOpRWS {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, io::Error>> {
        std::task::Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: std::pin::Pin<&mut Self>, _cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), io::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: std::pin::Pin<&mut Self>, _cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), io::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
}

impl AsyncSeek for MemoryNoOpRWS {
    fn start_seek(self: std::pin::Pin<&mut Self>, _position: io::SeekFrom) -> io::Result<()> {
        Ok(())
    }

    fn poll_complete(self: std::pin::Pin<&mut Self>, _cx: &mut std::task::Context<'_>) -> std::task::Poll<io::Result<u64>> {
        std::task::Poll::Ready(Ok(1u64))
    }
}
