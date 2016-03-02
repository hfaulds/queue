use std::thread;
use std::net::{TcpListener, TcpStream};
use std::io::{Write, BufRead, BufReader};
use std::sync::{Arc,Mutex};

fn handle_stream(stream: TcpStream, data: Arc<Mutex<u8>>) {
    let mut reader = BufReader::new(stream);

    loop {
        let mut buffer = Vec::new();
        let result = reader.read_until(b';', &mut buffer);

        if result.is_err() || buffer.last() != Some(&b';') {
            break;
        }

        let mut data = data.lock().unwrap();
        *data += 1;

        let mut stream = reader.get_mut();
        let _ = stream.write(format!("count {}\r\n",*data).as_bytes());
        let _ = stream.flush();
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
