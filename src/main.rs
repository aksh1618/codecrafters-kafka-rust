use std::io::Result;

mod api;
mod common;
mod server;

pub(crate) use common::buf;
pub(crate) use common::model;

fn main() -> Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");
    server::start_server().map(|server_thread| {
        server_thread
            .join()
            .expect("should be able to join server request handling thread");
    })
}
