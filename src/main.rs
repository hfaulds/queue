use std::thread;
use std::net::{TcpListener, TcpStream};
use std::io::{Write, BufRead, BufReader};
use std::sync::{Arc,Mutex};
use std::collections::LinkedList;

enum Command<'a> {
    Quit,
    Push(&'a str),
    Pop,
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

fn parse_cmd(buffer: &String) -> Result<Command,String> {
    let parts: Vec<&str> = buffer.trim().split(" ").collect();
    let cmd: &str = parts.first().unwrap();

    match cmd {
        "push" => {
            if parts.len() == 2 {
                let arg = parts.last().unwrap();
                Ok(Command::Push(arg))
            } else {
                Err("Too many arguments for PUSH".to_string())
            }
        }
        "pop" => Ok(Command::Pop),
        "quit" => Ok(Command::Quit),
        unknown => Err(format!("Unknown Command: {}", unknown))
    }
}

fn handle_stream(stream: TcpStream, data: Arc<Mutex<LinkedList<String>>>) {
    let mut reader = BufReader::new(stream);
    loop {
        let result = read_stream(&mut reader);
        if result.is_err() {
            break;
        }

        let mut stream = reader.get_mut();
        match parse_cmd(&result.unwrap()) {
            Ok(Command::Push(value)) => {
                let mut data = data.lock().unwrap();
                data.push_back(value.to_string());
                let _ = stream.write(b"SUCCESS");
            }
            Ok(Command::Pop) => {
                let mut data = data.lock().unwrap();
                match data.pop_front() {
                    Some(data) => { let _ = stream.write(format!("{}",data).as_bytes()); }
                    None => { let _ = stream.write(b"FAILURE"); }
                }
            }
            Ok(Command::Quit) => {
                let _ = stream.write(b"Bye bye\r\n");
                let _ = stream.flush();
                break;
            }
            Err(message) => {
                let _ = stream.write(message.as_bytes());
            }
        }
        let _ = stream.write(b"\r\n");
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
