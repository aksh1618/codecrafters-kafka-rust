use std::{
    io::{Result, Write},
    net::{TcpListener, TcpStream},
};

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
    stream.write_all(0i32.to_be_bytes().as_slice())?;
    stream.write_all(7i32.to_be_bytes().as_slice())?;
    stream.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Result},
        thread,
    };

    use super::*;

    #[test]
    fn test_response_v0_header() -> Result<()> {
        thread::spawn(|| {
            start_server()
                .unwrap_or_else(|_| panic!("Should be able to start the server on {ADDR}"));
        });
        let stream = std::net::TcpStream::connect(ADDR)?;
        let mut res_bytes = stream.bytes();
        let message_size_bytes = 0i32.to_be_bytes();
        for byte in message_size_bytes {
            println!("Received byte: {byte}");
            assert!(matches!(res_bytes.next(), Some(Ok(res_byte)) if res_byte == byte));
        }
        let header_bytes = 7i32.to_be_bytes();
        for byte in header_bytes {
            println!("Received byte: {byte}");
            assert!(matches!(res_bytes.next(), Some(Ok(res_byte)) if res_byte == byte));
        }
        Ok(())
    }
}
