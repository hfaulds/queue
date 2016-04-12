use std::thread;
use std::net::{TcpListener};

extern crate queue_experiments;
use queue_experiments::queue_table::{QueueTable};
use queue_experiments::connection::{Connection};

fn main() {
    let listener = TcpListener::bind("127.0.0.1:5248").unwrap();

    let queue_table = QueueTable::new();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let queue_table = queue_table.clone();
                thread::spawn(move|| {
                    Connection::new(&stream, &queue_table).listen();
                });
            }
            Err(_) => {
            }
        }
    }
}
