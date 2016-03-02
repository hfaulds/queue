use std::thread;
use std::net::{TcpListener, TcpStream};
use std::io::{Write, Read};
use std::sync::{Arc,Mutex};

fn handle_stream(mut stream: TcpStream, data: Arc<Mutex<u8>>) {
    loop {
        let mut buffer = [0; 10];
        let _ = stream.read(&mut buffer);

        {
            let mut data = data.lock().unwrap();
            *data += 1;

            let _ = stream.write(format!("{}",*data).as_bytes());
            let _ = stream.flush();
        }
    }
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:5248").unwrap();

    let data = Arc::new(Mutex::new(0));

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let data = data.clone();
                thread::spawn(move|| {
                    handle_stream(stream, data);
                });
            }
            Err(_) => {
            }
        }
    }
}
