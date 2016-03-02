use std::thread;
use std::net::{TcpListener, TcpStream};
use std::io::{Write, BufRead, BufReader};
use std::sync::{Arc,Mutex};

fn read_stream(reader: &mut BufReader<TcpStream>) -> Result<Vec<u8>,()> {
    let mut buffer = Vec::new();
    let result = reader.read_until(b';', &mut buffer);

    if result.is_err() {
        return Err(());
    } else if buffer.last() != Some(&b';') {
        return Err(());
    } else {
        return Ok(buffer);
    }
}

fn handle_stream(stream: TcpStream, data: Arc<Mutex<u8>>) {
    let mut reader = BufReader::new(stream);
    loop {
        let result = read_stream(&mut reader);
        if result.is_err() {
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
