use std::{
    io::{Read, Result, Write},
    net::{TcpListener, TcpStream},
};

use bytes::{Bytes, BytesMut};

const ADDR: &str = "127.0.0.1:9092";

fn main() -> Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");
    start_server()
}

fn start_server() -> Result<()> {
    let listener = TcpListener::bind(ADDR)?;
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                handle_connection_response_v0(stream)?;
            }
            Err(e) => {
                println!("error: {e}");
            }
        }
    }
    Ok(())
}

fn handle_connection_response_v0(mut stream: TcpStream) -> Result<()> {
    let request_correlation_id = read_correlation_id_request_v2(&mut stream)?;
    stream.write_all(0i32.to_be_bytes().as_slice())?;
    stream.write_all(request_correlation_id.as_ref())?;
    stream.flush()?;
    Ok(())
}

fn read_correlation_id_request_v2(stream: &mut TcpStream) -> Result<Bytes> {
    // 12 bytes: 4 message size | 2 api key | 2 api version | 4 correlation id
    // See: https://kafka.apache.org/protocol.html
    let mut buf = BytesMut::zeroed(12);
    stream.read_exact(&mut buf)?;
    println!("read 12 bytes");
    let mut bytes = buf.freeze();
    let message_size = bytes
        .split_to(4)
        .as_ref()
        .try_into()
        .map(i32::from_be_bytes)
        .expect("message size should be 4 bytes");
    println!("message size: {message_size}");
    let correlation_id = bytes.slice(4..);
    let remaining_bytes = (message_size - 8)
        .try_into()
        .expect("message size shouldn't be negative");
    let mut discardable = BytesMut::zeroed(remaining_bytes);
    stream.read_exact(discardable.as_mut())?;
    println!("read remaining {remaining_bytes} bytes");
    Ok(correlation_id)
}

#[cfg(test)]
mod tests {
    use indoc::indoc;

    use super::*;
    use std::sync::Once;
    use std::{
        io::{Read, Result},
        process::Command,
        thread,
    };

    static INIT: Once = Once::new();

    pub fn ensure_server_running() {
        INIT.call_once(|| {
            thread::spawn(|| {
                start_server().unwrap_or_else(|e| {
                    panic!("Should be able to start the server on {ADDR}: {e}")
                });
            });
        });
    }

    #[test]
    fn test_cmd_request_v2_response_v0_correlation_id() -> Result<()> {
        ensure_server_running();
        let output = Command::new("sh")
            .arg("-c")
            .arg("echo -n '00000023001200046f7fc66100096b61666b612d636c69000a6b61666b612d636c6904302e3100' | xxd -r -p | nc localhost 9092 | hexdump -C")
            .output()?;
        assert!(output.status.success());
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        println!("{stdout:?}");
        println!("{stderr:?}");
        assert!(stderr.is_empty());
        assert_eq!(
            stdout,
            indoc! {"
             00000000  00 00 00 00 6f 7f c6 61                           |....o..a|
             00000008
            "}
        );
        Ok(())
    }

    #[test]
    fn test_request_v2_response_v0_correlation_id() -> Result<()> {
        #[allow(clippy::unreadable_literal)]
        // const CORRELATION_ID: i32 = 1870644833i32;
        const CORRELATION_ID: i32 = 1234567890i32;
        ensure_server_running();
        let mut stream = std::net::TcpStream::connect(ADDR)?;
        stream.write_all(10i32.to_be_bytes().as_slice())?;
        stream.write_all(0i16.to_be_bytes().as_slice())?;
        stream.write_all(0i16.to_be_bytes().as_slice())?;
        stream.write_all(CORRELATION_ID.to_be_bytes().as_slice())?;
        stream.write_all((-1_i16).to_be_bytes().as_slice())?;
        stream.write_all(0i8.to_be_bytes().as_slice())?;
        stream.flush()?;
        println!("Wrote and flushed all bytes");
        let mut res_bytes = stream.bytes();
        let message_size_bytes = 0i32.to_be_bytes();
        for byte in message_size_bytes {
            println!("Received byte: {byte}");
            assert!(matches!(res_bytes.next(), Some(Ok(res_byte)) if res_byte == byte));
        }
        let header_bytes = CORRELATION_ID.to_be_bytes();
        for byte in header_bytes {
            println!("Received byte: {byte}");
            assert!(matches!(res_bytes.next(), Some(Ok(res_byte)) if res_byte == byte));
        }
        Ok(())
    }
}
