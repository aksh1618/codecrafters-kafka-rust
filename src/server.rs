use bytes::Buf as _;
use std::{
    io::{Read as _, Result, Write as _},
    net::{TcpListener, TcpStream},
    thread::{self, JoinHandle},
};

use crate::{
    api::{self, RequestV2},
    buf::BufExt as _,
};

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
    dbg!(&request_message_v2);
    let correlation_id = request_message_v2.header.correlation_id;
    let response = api::handle_request(request_message_v2);
    let message_size = response.message_size;
    stream.write_all(&response.into_bytes())?;
    stream.flush()?;
    println!("Wrote and flushed {message_size}(+4) response bytes for {correlation_id}");
    Ok(())
}

#[expect(
    clippy::shadow_unrelated,
    reason = "Sticking to consistent naming for readability"
)]
fn read_request_v2(stream: &mut TcpStream) -> Result<RequestV2> {
    // read size
    let mut buf = [0; 4];
    stream.read_exact(&mut buf)?;
    println!("read 4 bytes for message size");
    let mut bytes = buf.as_slice();
    let size = bytes.get_i32();
    println!("request message size: {size}");

    // read and decode message
    let message_size = size.try_into().expect("message size shouldn't be negative");
    let mut buf = vec![0u8; message_size];
    stream.read_exact(&mut buf)?;
    let mut bytes = buf.as_slice();
    let message = bytes.get_decoded();
    Ok(RequestV2 {
        message_size: size,
        message,
    })
}

#[allow(
    clippy::restriction,
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::{
        api::{ApiKind, Response, ResponseMessage},
        buf::{BufMutExt as _, Encode},
        model::ApiVersion,
        server::{self, ADDR},
    };
    use bytes::{BufMut as _, BytesMut};
    use indoc::indoc;
    use std::{
        io::Result,
        net,
        process::Command,
        sync::{Mutex, Once},
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

    #[expect(clippy::unreadable_literal)]
    pub fn perform_request(
        api_kind: ApiKind,
        api_version: ApiVersion,
        api_request: &impl Encode,
    ) -> Result<Response> {
        // TODO: Switch to random i32?
        const CORRELATION_ID: i32 = 215707621i32;
        const CLIENT_ID: &str = "kafka-tester";
        let mut payload = BytesMut::new();
        payload.put_encoded(api_request);
        let payload = payload.freeze();
        let request = RequestV2::new(api_kind, api_version, CORRELATION_ID, CLIENT_ID, payload);
        ensure_server_running();
        let mut stream = net::TcpStream::connect(ADDR)?;
        let api_kind = request.message.header.api_key.try_into().unwrap();
        let correlation_id = request.message.header.correlation_id;
        let bytes = &request.into_bytes();
        stream.write_all(bytes)?;
        let mut buf = [0; 4];
        stream.read_exact(&mut buf)?;
        println!("read 4 bytes for message size");
        let mut bytes = buf.as_slice();
        let size = bytes.get_i32();
        println!("response message size: {size}");
        let message_size = size.try_into().expect("message size shouldn't be negative");
        let mut buf = vec![0u8; message_size];
        stream.read_exact(&mut buf)?;
        let mut bytes = buf.as_slice();
        let response_message = ResponseMessage::decode(api_kind, &mut bytes);
        dbg!(&response_message);
        assert_eq!(response_message.correlation_id(), correlation_id);
        Ok(Response::new(response_message))
    }
}
