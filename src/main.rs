use std::thread;
use std::net::{TcpListener, TcpStream};
use std::io::{Write, BufRead, BufReader};
use std::sync::{Arc,Mutex};
use std::collections::LinkedList;

enum Command<'a> {
    Quit,
    Push,
    Pop,
    Unknown(&'a str),
}

fn read_stream(reader: &mut BufReader<TcpStream>) -> Result<String,()> {
    let mut buffer = Vec::new();
    let result = reader.read_until(b';', &mut buffer);

    if result.is_err() {
        return Err(());
    }

    if buffer.pop() != Some(b';') {
        return Err(());
    }

    let result = String::from_utf8(buffer);
    match result {
        Ok(str) => Ok(str),
        Err(_) => Err(())
    }
}

fn parse_cmd(buffer: &str) -> Command {
    match buffer.trim() {
        "push" => Command::Push,
        "pop" => Command::Pop,
        "quit" => Command::Quit,
        unknown => Command::Unknown(unknown)
    }
}

fn handle_stream(stream: TcpStream, data: Arc<Mutex<LinkedList<u8>>>) {
    let mut reader = BufReader::new(stream);
    loop {
        let result = read_stream(&mut reader);
        if result.is_err() {
            break;
        }

        let mut stream = reader.get_mut();
        match parse_cmd(&result.unwrap()) {
            Command::Push => {
                let mut data = data.lock().unwrap();
                data.push_back(1);
                let _ = stream.write(b"SUCCESS\r\n");
            }
            Command::Pop => {
                let mut data = data.lock().unwrap();
                match data.pop_front() {
                    Some(data) => { let _ = stream.write(format!("{}\r\n",data).as_bytes()); }
                    None => { let _ = stream.write(b"FAILURE\r\n"); }
                }
            }
            Command::Quit => {
                let _ = stream.write(b"Bye bye\r\n");
                let _ = stream.flush();
                break;
            }
            Command::Unknown(command) => {
                let _ = stream.write(format!("Unkown command {}\r\n", command).as_bytes());
            }
        }
        let _ = stream.flush();
    }
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:5248").unwrap();

    let data = Arc::new(Mutex::new(LinkedList::new()));

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
