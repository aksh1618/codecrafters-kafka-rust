#![allow(
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
use std::{
    io::{Read, Result, Write},
    net::{TcpListener, TcpStream},
    thread::{self, JoinHandle},
};

use bytes::{Buf, BufMut};

const ADDR: &str = "127.0.0.1:9092";

#[derive(Debug)]
struct RequestMessage {
    correlation_id: i32,
    #[allow(dead_code)]
    api_key: i16,
    api_version: i16,
}

fn main() -> Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");
    start_server()?
        .join()
        .expect("should be able to join server request handling thread");
    Ok(())
}

fn start_server() -> Result<JoinHandle<()>> {
    let listener = TcpListener::bind(ADDR)?;
    let handler_thread = thread::spawn(move || {
        for stream in listener.incoming() {
            if let Err(e) = stream.map(handle_connection_response_v0) {
                println!("Encountered error: {e}");
            }
        }
    });
    Ok(handler_thread)
}

fn handle_connection_response_v0(mut stream: TcpStream) -> Result<()> {
    let RequestMessage {
        correlation_id,
        api_version,
        ..
    } = read_request_v2(&mut stream)?;
    let error_code = if (0..=4).contains(&api_version) {
        0i16
    } else {
        35i16
    };
    let num_api_keys = 1u8;
    let apiversions_api_key = 18i16;
    let apiversions_min_version = 4i16;
    let apiversions_max_version = 4i16;
    let throttle_time_ms = 0i32;
    let tag_buffer = 0i8;
    let mut buf = vec![];
    buf.put_i32(correlation_id);
    buf.put_i16(error_code);
    buf.put_u8(num_api_keys + 1);
    buf.put_i16(apiversions_api_key);
    buf.put_i16(apiversions_min_version);
    buf.put_i16(apiversions_max_version);
    buf.put_i8(tag_buffer);
    buf.put_i32(throttle_time_ms);
    buf.put_i8(tag_buffer);
    let message_size = buf.len();
    stream.write_all((message_size as i32).to_be_bytes().as_slice())?;
    stream.write_all(&buf)?;
    stream.flush()
}

fn read_request_v2(stream: &mut TcpStream) -> Result<RequestMessage> {
    // 12 bytes: 4 message size | 2 api key | 2 api version | 4 correlation id
    // See: https://kafka.apache.org/protocol.html
    let mut buf = [0u8; 12];
    stream.read_exact(&mut buf)?;
    println!("read 12 bytes");
    let mut bytes = buf.as_slice();
    let message_size = bytes.get_i32();
    println!("request message size: {message_size}");
    let api_key = bytes.get_i16();
    let api_version = bytes.get_i16();
    let correlation_id = bytes.get_i32();
    // We don't care about rest of the message for now
    let remaining_bytes = (message_size - 8)
        .try_into()
        .expect("message size shouldn't be negative");
    let mut discardable = vec![0u8; remaining_bytes];
    stream.read_exact(&mut discardable)?;
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
    use std::sync::{Mutex, Once};
    use std::{
        io::{Read, Result},
        process::Command,
    };

    static INIT: Once = Once::new();
    static INIT_RESULT: Mutex<Option<Result<()>>> = Mutex::new(None);

    #[allow(clippy::unwrap_used)]
    pub fn ensure_server_running() {
        INIT.call_once(|| {
            let result = start_server();
            *INIT_RESULT.lock().expect("lock") = Some(result.map(|_| ()));
        });
        let lock = INIT_RESULT.lock().unwrap();
        let server_start_result = lock.as_ref().unwrap();
        let successfully_started = server_start_result.is_ok();
        assert!(
            successfully_started,
            "Server initialization panicked: {server_start_result:?}"
        );
    }

    #[test]
    fn test_cmd_apiversions_v4_request_response() -> Result<()> {
        ensure_server_running();
        let output = Command::new("sh")
            .arg("-c")
            .arg("echo -n '00000023001200043db96a2800096b61666b612d636c69000a6b61666b612d636c6904302e3100' | xxd -r -p | nc localhost 9092 | hexdump -C")
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
                00000000  00 00 00 13 3d b9 6a 28  00 00 02 00 12 00 04 00  |....=.j(........|
                00000010  04 00 00 00 00 00 00                              |.......|
                00000017
            "}
        );
        Ok(())
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
                00000000  00 00 00 13 4f 74 d2 8b  00 23 02 00 12 00 04 00  |....Ot...#......|
                00000010  04 00 00 00 00 00 00                              |.......|
                00000017
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
                00000000  00 00 00 13 6f 7f c6 61  00 00 02 00 12 00 04 00  |....o..a........|
                00000010  04 00 00 00 00 00 00                              |.......|
                00000017
            "}
        );
        Ok(())
    }

    #[test]
    fn test_request_header_v2_response_header_v0_correlation_id() -> Result<()> {
        #[allow(clippy::unreadable_literal)]
        // const CORRELATION_ID: i32 = 1870644833i32;
        const CORRELATION_ID: i32 = 1234567890i32;
        ensure_server_running();
        let mut stream = std::net::TcpStream::connect(ADDR)?;
        let mut buf = vec![];
        buf.put_i16(0i16);
        buf.put_i16(0i16);
        buf.put_i32(CORRELATION_ID);
        buf.put_i16(-1i16);
        buf.put_i8(0i8);
        stream.write_all((buf.len() as i32).to_be_bytes().as_slice())?;
        stream.write_all(&buf)?;
        stream.flush()?;
        println!("Wrote and flushed all request bytes");
        let mut buf = [0u8; 8];
        stream.read_exact(&mut buf)?;
        let mut bytes = buf.as_slice();
        let _message_size = bytes.get_i32();
        let correlation_id = bytes.get_i32();
        assert_eq!(correlation_id, CORRELATION_ID);
        Ok(())
    }

    #[test]
    fn test_apiversions_v4_request_response() -> Result<()> {
        #[allow(clippy::unreadable_literal)]
        const CORRELATION_ID: i32 = 1857043921i32;
        ensure_server_running();
        let mut stream = std::net::TcpStream::connect(ADDR)?;
        let mut buf = vec![];
        buf.put_i16(18i16);
        buf.put_i16(4i16);
        buf.put_i32(CORRELATION_ID);
        buf.put_i16(-1i16);
        buf.put_i8(0i8);
        stream.write_all((buf.len() as i32).to_be_bytes().as_slice())?;
        stream.write_all(&buf)?;
        stream.flush()?;
        println!("Wrote and flushed all request bytes");

        let mut buf = [0u8; 4];
        stream.read_exact(&mut buf)?;
        let mut bytes = buf.as_slice();
        let message_size = bytes.get_i32();
        println!("Received response message size: {message_size}");
        let remaining_bytes = message_size as usize;
        let mut buf = vec![0u8; remaining_bytes];
        stream.read_exact(&mut buf)?;
        let mut bytes = buf.as_slice();
        let correlation_id = bytes.get_i32();
        assert_eq!(correlation_id, CORRELATION_ID);
        let error_code = bytes.get_i16();
        assert_eq!(error_code, 0i16);
        let num_api_keys = bytes.get_u8() - 1;
        assert_eq!(num_api_keys, 1u8);
        let apiversions_api_key = bytes.get_i16();
        assert_eq!(apiversions_api_key, 18i16);
        let apiversions_min_version = bytes.get_i16();
        assert_eq!(apiversions_min_version, 4i16);
        let apiversions_max_version = bytes.get_i16();
        let tag_buffer = bytes.get_i8();
        assert_eq!(tag_buffer, 0i8);
        assert_eq!(apiversions_max_version, 4i16);
        let throttle_time_ms = bytes.get_i32();
        assert_eq!(throttle_time_ms, 0i32);
        let tag_buffer = bytes.get_i8();
        assert_eq!(tag_buffer, 0i8);
        assert_eq!(bytes.len(), 0);
        Ok(())
    }
}
