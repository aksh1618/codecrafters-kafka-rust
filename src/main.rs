use std::{
    io::{Read, Result, Write},
    net::{TcpListener, TcpStream},
};

use bytes::{Bytes, BytesMut};

const ADDR: &str = "127.0.0.1:9092";

#[derive(Debug)]
struct RequestMessage {
    correlation_id: Bytes,
    #[allow(dead_code)]
    api_key: i16,
    api_version: i16,
}

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
    let RequestMessage {
        correlation_id,
        api_version,
        ..
    } = read_correlation_id_request_v2(&mut stream)?;
    let error_code = if (0..=4).contains(&api_version) {
        0i16
    } else {
        35i16
    };
    stream.write_all(0i32.to_be_bytes().as_slice())?;
    stream.write_all(correlation_id.as_ref())?;
    stream.write_all(error_code.to_be_bytes().as_slice())?;
    stream.flush()?;
    Ok(())
}

macro_rules! let_consuming {
    ($var:ident: $T:ty = $bytes: expr) => {
        let $var = {
            let size = std::mem::size_of::<$T>();
            $bytes
                .split_to(size)
                .as_ref()
                .try_into()
                .map(<$T>::from_be_bytes)
                .unwrap_or_else(|e| {
                    panic!(
                        "expected to read & parse {size} bytes for {}: {e}",
                        stringify!($var)
                    )
                })
        };
    };
}

fn read_correlation_id_request_v2(stream: &mut TcpStream) -> Result<RequestMessage> {
    // 12 bytes: 4 message size | 2 api key | 2 api version | 4 correlation id
    // See: https://kafka.apache.org/protocol.html
    let mut buf = BytesMut::zeroed(12);
    stream.read_exact(&mut buf)?;
    println!("read 12 bytes");
    let mut bytes = buf.freeze();
    let_consuming!(message_size: i32 = bytes);
    println!("message size: {message_size}");
    let_consuming!(api_key: i16 = bytes);
    let_consuming!(api_version: i16 = bytes);
    let correlation_id = bytes.split_to(4);
    // We don't care about rest of the message for now
    let remaining_bytes = (message_size - 8)
        .try_into()
        .expect("message size shouldn't be negative");
    let mut discardable = BytesMut::zeroed(remaining_bytes);
    stream.read_exact(discardable.as_mut())?;
    println!("read remaining {remaining_bytes} bytes");
    let request_message = RequestMessage {
        correlation_id,
        api_key,
        api_version,
    };
    println!("Parsed request message: {request_message:?}");
    Ok(request_message)
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
    fn test_cmd_request_v2_response_v0_correlation_id_incorrect_version_id() -> Result<()> {
        ensure_server_running();
        let output = Command::new("sh")
            .arg("-c")
            .arg("echo -n '000000230012674a4f74d28b00096b61666b612d636c69000a6b61666b612d636c6904302e3100' | xxd -r -p | nc localhost 9092 | hexdump -C")
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
                00000000  00 00 00 00 4f 74 d2 8b  00 23                    |....Ot...#|
                0000000a
            "}
        );
        Ok(())
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
                00000000  00 00 00 00 6f 7f c6 61  00 00                    |....o..a..|
                0000000a
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
