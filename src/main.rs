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
            Ok(_stream) => {
                let f = BufReader::new(_stream);
                println!("accepted new connection");
                println!("Received request:\n");
                let bytes = f.bytes().collect::<Result<Vec<_>, _>>().unwrap();
                print!("{}", String::from_utf8(bytes).unwrap());
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
