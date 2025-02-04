use bytes::Buf as _;
use std::{
    io::{Read as _, Result, Write as _},
    net::{TcpListener, TcpStream},
    thread::{self, JoinHandle},
};

use crate::api::{self, RequestHeaderV2, RequestMessageV2, RequestV2};
// For one-shot write
// use bytes::{Buf as _, BufMut as _, Bytes};

pub const ADDR: &str = "127.0.0.1:9092";

pub fn start_server() -> Result<JoinHandle<()>> {
    let listener = TcpListener::bind(ADDR)?;
    let handler_thread = thread::spawn(move || {
        for stream in listener.incoming() {
            thread::spawn(move || {
                if let Err(e) = stream.map(handle_connection_response_v0) {
                    println!("Encountered error: {e}");
                };
            });
        }
    });
    Ok(handler_thread)
}

fn handle_connection_response_v0(mut stream: TcpStream) -> Result<()> {
    loop {
        if let Err(e) = read_next_request_for_response_v0(&mut stream) {
            println!("Closed connection: [{}: {e}]", e.kind());
            break Ok(());
        };
    }
}

fn read_next_request_for_response_v0(stream: &mut TcpStream) -> Result<()> {
    let request_message_v2 = read_request_v2(stream)?.message;
    let correlation_id = request_message_v2.header.correlation_id;
    let buf = api::handle_request(request_message_v2);
    let message_size = i32::try_from(size_of_val(&correlation_id) + buf.len())
        .expect("message size shouldn't be large enough to wrap around when converted to i32");
    // Simpler but multi-shot write
    stream.write_all((message_size).to_be_bytes().as_slice())?;
    stream.write_all((correlation_id).to_be_bytes().as_slice())?;
    stream.write_all(&buf)?;
    stream.flush()?;
    // // One-shot but more complex write
    // let response_size = size_of_val(&message_size) + message_size as usize;
    // let response = (message_size.to_be_bytes().as_slice())
    //     .chain(correlation_id.to_be_bytes().as_slice())
    //     .chain(buf)
    //     .copy_to_bytes(response_size);
    // stream.write_all(&response)?;
    // TODO: Decide whether this one-shot write will be more performant
    println!("Wrote and flushed {message_size}(+4) response bytes for {correlation_id}");
    Ok(())
}

/// 4 bytes: message size
/// 8 bytes: header (2 api key | 2 api version | 4 correlation id)
/// Rest: Payload
/// See: [Kafka protocol docs](https://kafka.apache.org/protocol.html)
fn read_request_v2(stream: &mut TcpStream) -> Result<RequestV2> {
    let mut buf = [0; 4];
    stream.read_exact(&mut buf)?;
    println!("read 4 bytes for message size");
    let mut bytes = buf.as_slice();
    let size = bytes.get_i32();
    println!("request message size: {size}");
    let header = read_request_header_v2(stream)?;
    let remaining_bytes = (size - 8)
        .try_into()
        .expect("message size shouldn't be negative");
    let mut payload = vec![0u8; remaining_bytes];
    stream.read_exact(&mut payload)?;
    let payload = payload.into();
    println!("read remaining {remaining_bytes} bytes");
    let message = RequestMessageV2 { header, payload };
    Ok(RequestV2 { size, message })
}

/// 8 bytes: 2 api key | 2 api version | 4 correlation id
/// See: [Kafka protocol docs](https://kafka.apache.org/protocol.html)
fn read_request_header_v2(stream: &mut TcpStream) -> Result<RequestHeaderV2> {
    let mut buf = [0; 8];
    stream.read_exact(&mut buf)?;
    println!("read 8 bytes for header");
    let mut bytes = buf.as_slice();
    let api_key = bytes.get_i16();
    let api_version = bytes.get_i16();
    let correlation_id = bytes.get_i32();
    let header = RequestHeaderV2 {
        api_key,
        api_version,
        correlation_id,
    };
    println!("Parsed request message: {header:?}");
    Ok(header)
}

#[allow(
    clippy::restriction,
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::{self, ADDR};
    use bytes::BufMut as _;
    use indoc::indoc;
    use std::{
        io::Result,
        net,
        process::Command,
        sync::{Mutex, Once},
        thread,
        time::Instant,
    };

    static INIT: Once = Once::new();
    static INIT_RESULT: Mutex<Option<Result<()>>> = Mutex::new(None);

    pub fn ensure_server_running() {
        INIT.call_once(|| {
            let result = server::start_server();
            *INIT_RESULT.lock().expect("lock") = Some(result.map(|_| ()));
        });
        if let Err(e) = INIT_RESULT.lock().unwrap().as_ref().unwrap() {
            panic!("Server initialization panicked: {e}");
        };
    }

    // #[test]
    #[expect(dead_code)]
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

    // #[test]
    #[expect(dead_code)]
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

    // #[test]
    #[expect(dead_code)]
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
        #[expect(clippy::unreadable_literal)]
        // const CORRELATION_ID: i32 = 1870644833i32;
        const CORRELATION_ID: i32 = 1234567890i32;
        ensure_server_running();
        let mut stream = net::TcpStream::connect(ADDR)?;
        let mut buf = vec![];
        buf.put_i16(18i16);
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
        #[expect(clippy::unreadable_literal)]
        const CORRELATION_ID: i32 = 1857043921i32;
        ensure_server_running();
        let mut stream = net::TcpStream::connect(ADDR)?;
        test_apiversions_v4_request_response_for_correlation_id(&mut stream, CORRELATION_ID)
    }

    fn test_apiversions_v4_request_response_for_correlation_id(
        stream: &mut TcpStream,
        correlation_id: i32,
    ) -> Result<()> {
        let mut buf = vec![];
        buf.put_i16(18i16);
        buf.put_i16(4i16);
        buf.put_i32(correlation_id);
        buf.put_i16(-1i16);
        buf.put_i8(0i8);
        stream.write_all((buf.len() as i32).to_be_bytes().as_slice())?;
        stream.write_all(&buf)?;
        stream.flush()?;
        println!("Wrote and flushed all request bytes for {correlation_id}");

        let mut buf = [0u8; 4];
        stream.read_exact(&mut buf)?;
        let mut bytes = buf.as_slice();
        let message_size = bytes.get_i32();
        println!("Received response message size: {message_size}");
        let remaining_bytes = message_size as usize;
        let mut buf = vec![0u8; remaining_bytes];
        stream.read_exact(&mut buf)?;
        let mut bytes = buf.as_slice();
        let res_correlation_id = bytes.get_i32();
        assert_eq!(res_correlation_id, correlation_id);
        let error_code = bytes.get_i16();
        assert_eq!(error_code, 0i16);
        let num_api_keys = bytes.get_u8() - 1;
        assert_eq!(num_api_keys, 2u8);
        let apiversions_api_key = bytes.get_i16();
        assert_eq!(apiversions_api_key, 18i16);
        let apiversions_min_version = bytes.get_i16();
        assert_eq!(apiversions_min_version, 4i16);
        let apiversions_max_version = bytes.get_i16();
        assert_eq!(apiversions_max_version, 4i16);
        let tag_buffer = bytes.get_i8();
        assert_eq!(tag_buffer, 0i8);
        let describe_topic_partitions_api_key = bytes.get_i16();
        assert_eq!(describe_topic_partitions_api_key, 75i16);
        let describe_topic_partitions_min_version = bytes.get_i16();
        assert_eq!(describe_topic_partitions_min_version, 0i16);
        let describe_topic_partitions_max_version = bytes.get_i16();
        assert_eq!(describe_topic_partitions_max_version, 0i16);
        let tag_buffer = bytes.get_i8();
        assert_eq!(tag_buffer, 0i8);
        let throttle_time_ms = bytes.get_i32();
        assert_eq!(throttle_time_ms, 0i32);
        let tag_buffer = bytes.get_i8();
        assert_eq!(tag_buffer, 0i8);
        assert_eq!(bytes.len(), 0);
        println!(
            "Read {} response bytes for {res_correlation_id}",
            message_size + 4
        );
        Ok(())
    }

    #[test]
    #[expect(clippy::unreadable_literal)]
    fn test_multiple_serial_apiversions_v4_request_response() -> Result<()> {
        const CORRELATION_ID_1: i32 = 1956054920i32;
        const CORRELATION_ID_2: i32 = 205506591i32;
        const CORRELATION_ID_3: i32 = 215707621i32;
        ensure_server_running();
        let mut stream = net::TcpStream::connect(ADDR)?;
        test_apiversions_v4_request_response_for_correlation_id(&mut stream, CORRELATION_ID_1)?;
        test_apiversions_v4_request_response_for_correlation_id(&mut stream, CORRELATION_ID_2)?;
        test_apiversions_v4_request_response_for_correlation_id(&mut stream, CORRELATION_ID_3)?;
        Ok(())
    }

    // TODO: Improve this test, can be flaky
    #[test]
    #[expect(clippy::unreadable_literal)]
    fn test_multiple_parallel_apiversions_v4_request_response() {
        const CORRELATION_IDS: [i32; 5] = [
            1956054920i32,
            205506591i32,
            215707621i32,
            1857043921i32,
            1857043922i32,
        ];
        let repetitions = CORRELATION_IDS.len();
        ensure_server_running();
        let mut threads = Vec::new();
        let start_time = Instant::now();
        for correlation_id in CORRELATION_IDS {
            let _ = net::TcpStream::connect(ADDR).map(|mut stream| {
                test_apiversions_v4_request_response_for_correlation_id(&mut stream, correlation_id)
            });
        }
        let time_taken_ser = start_time.elapsed();
        println!("Time taken for 3 serial request: {time_taken_ser:?}");
        let start_time = Instant::now();
        for correlation_id in CORRELATION_IDS {
            for _ in 0..repetitions {
                threads.push(thread::spawn(move || {
                    let _ = net::TcpStream::connect(ADDR).map(|mut stream| {
                        test_apiversions_v4_request_response_for_correlation_id(
                            &mut stream,
                            correlation_id,
                        )
                    });
                    // thread::sleep(std::time::Duration::from_millis(100));
                }));
            }
        }
        for thread in threads {
            thread.join().expect("should be able to join thread");
        }
        let time_taken_par = start_time.elapsed();
        assert!(time_taken_par < time_taken_ser * repetitions as u32);
        println!("Time taken for 3 parallel requests: {time_taken_par:?}");
    }
}
