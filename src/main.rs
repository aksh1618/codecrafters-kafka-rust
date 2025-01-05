#![allow(unused_imports)]
use std::{
    io::{BufReader, Read},
    net::TcpListener,
};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                // let f = BufReader::new(_stream);
                println!("accepted new connection");
                let mut content = String::new();
                let bytes_read = _stream.read_to_string(&mut content).unwrap();
                // let bytes = f.bytes().collect::<Result<Vec<_>, _>>().unwrap();
                // print!("{}", String::from_utf8(bytes).unwrap());
                println!("Received {} bytes in request:", bytes_read);
                println!("{}", content);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
