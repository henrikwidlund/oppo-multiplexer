use futures_lite::future;
use smol::{
    Timer,
    io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt},
    net::TcpStream,
};
use socket2::{SockRef, TcpKeepalive};
use std::time::Duration;

/// Hard cap on a single `\r`-terminated line from either a client or the
/// backend. Prevents a peer that never sends `\r` from growing the read
/// buffer without bound (OOM/DoS). Oppo protocol lines are <100 bytes in
/// practice.
pub const MAX_LINE_LEN: usize = 4096;
const BACKEND_WRITE_TIMEOUT: Duration = Duration::from_secs(2);
const TCP_KEEPALIVE_TIME: Duration = Duration::from_secs(30);
const TCP_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
const TCP_KEEPALIVE_RETRIES: u32 = 3;

/// Enables TCP keepalive on a socket, so a peer that goes dark without a
/// clean FIN/RST (device sleeps, Wi-Fi drops, power loss,
/// silent black-hole) is detected and the read loop errors out instead of
/// blocking forever on a half-open socket — which would otherwise leak a
/// client's `MAX_CLIENTS` slot, or hide a dead backend, until the OS default
/// keepalive (2h on Linux) finally kicks in.
pub fn enable_tcp_keepalive(stream: &TcpStream) -> std::io::Result<()> {
    let keepalive = TcpKeepalive::new()
        .with_time(TCP_KEEPALIVE_TIME)
        .with_interval(TCP_KEEPALIVE_INTERVAL)
        .with_retries(TCP_KEEPALIVE_RETRIES);
    SockRef::from(stream).set_tcp_keepalive(&keepalive)
}

/// Bounded variant of `AsyncBufReadExt::read_until`: appends bytes into `buf`
/// until `delim` is found or EOF, but fails with `InvalidData` if the line
/// would exceed `max` bytes. Prevents a peer that never sends `delim` from
/// growing `buf` without bound.
pub async fn read_until_capped<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    delim: u8,
    buf: &mut Vec<u8>,
    max: usize,
) -> std::io::Result<usize> {
    let start = buf.len();
    loop {
        let (consumed, found) = {
            let available = reader.fill_buf().await?;
            if available.is_empty() {
                return Ok(buf.len() - start);
            }
            let (n, found) = available
                .iter()
                .position(|&b| b == delim)
                .map_or((available.len(), false), |i| (i + 1, true));
            if buf.len() - start + n > max {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "line exceeded max length",
                ));
            }
            buf.extend_from_slice(&available[..n]);
            (n, found)
        };
        std::pin::Pin::new(&mut *reader).consume(consumed);
        if found {
            return Ok(buf.len() - start);
        }
    }
}

/// Writes `data` to `stream`, failing with `TimedOut` if the kernel does not
/// accept it within `BACKEND_WRITE_TIMEOUT`. A stuck TCP send buffer would
/// otherwise block the broker indefinitely; on timeout the caller drops the
/// connection (since a partial write may have already landed).
pub async fn write_with_timeout(stream: &mut TcpStream, data: &[u8]) -> std::io::Result<()> {
    future::or(
        async { stream.write_all(data).await },
        async {
            Timer::after(BACKEND_WRITE_TIMEOUT).await;
            Err::<(), _>(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "backend write timed out",
            ))
        },
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_lite::io::Cursor;
    use smol::io::BufReader;

    #[test]
    fn read_until_capped_reads_complete_line() {
        let input: &[u8] = b"#QPW\rrest";
        let mut reader = BufReader::with_capacity(64, Cursor::new(input));
        let mut buf = Vec::new();
        let n = future::block_on(read_until_capped(&mut reader, b'\r', &mut buf, 4096)).unwrap();
        assert_eq!(n, 5);
        assert_eq!(buf.as_slice(), b"#QPW\r");
    }

    #[test]
    fn read_until_capped_returns_zero_on_immediate_eof() {
        let mut reader = BufReader::with_capacity(64, Cursor::new(&[][..]));
        let mut buf = Vec::new();
        let n = future::block_on(read_until_capped(&mut reader, b'\r', &mut buf, 4096)).unwrap();
        assert_eq!(n, 0);
        assert!(buf.is_empty());
    }

    #[test]
    fn read_until_capped_truncated_line_returns_partial_bytes() {
        let input: &[u8] = b"@UPW 0";
        let mut reader = BufReader::with_capacity(64, Cursor::new(input));
        let mut buf = Vec::new();
        let n = future::block_on(read_until_capped(&mut reader, b'\r', &mut buf, 4096)).unwrap();
        assert_eq!(n, 6);
        assert_eq!(buf.last(), Some(&b'0'));
        assert_ne!(buf.last(), Some(&b'\r'));
    }

    #[test]
    fn read_until_capped_rejects_oversized_line() {
        // 100 bytes of 'a' then \r, with max=50 — must error before crossing the cap.
        let mut input = vec![b'a'; 100];
        input.push(b'\r');
        let mut reader = BufReader::with_capacity(16, Cursor::new(input));
        let mut buf = Vec::new();
        let err = future::block_on(read_until_capped(&mut reader, b'\r', &mut buf, 50))
            .expect_err("expected InvalidData on oversize line");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn read_until_capped_spans_multiple_fill_buf_calls() {
        // BufReader capacity 4 forces multiple fill_buf rounds before \r is found.
        let input: &[u8] = b"abcdefghij\r";
        let mut reader = BufReader::with_capacity(4, Cursor::new(input));
        let mut buf = Vec::new();
        let n = future::block_on(read_until_capped(&mut reader, b'\r', &mut buf, 64)).unwrap();
        assert_eq!(n, 11);
        assert_eq!(buf.as_slice(), b"abcdefghij\r");
    }
}
