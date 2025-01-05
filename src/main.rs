#![allow(unused_imports)]
use std::{
    io::{Result, Write},
    net::TcpListener,
};

fn main() -> Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                println!("accepted new connection");
                _stream.write_all(0i32.to_be_bytes().as_slice()).unwrap();
                _stream.write_all(7i32.to_be_bytes().as_slice()).unwrap();
                _stream.flush()?;
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use super::*;

    #[test]
    fn test_response_v0_header() {
        let stream = std::net::TcpStream::connect("127.0.0.1:9092")
            .expect("The program should be running on port 9092 before the test is run");
        let mut res_bytes = stream.bytes();
        let message_size_bytes = 0i32.to_be_bytes();
        for byte in message_size_bytes {
            println!("Received byte: {}", byte);
            assert!(matches!(res_bytes.next(), Some(Ok(res_byte)) if res_byte == byte));
        }
        let header_bytes = 7i32.to_be_bytes();
        for byte in header_bytes {
            println!("Received byte: {}", byte);
            assert!(matches!(res_bytes.next(), Some(Ok(res_byte)) if res_byte == byte));
        }
    }
}
