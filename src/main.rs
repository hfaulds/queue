use std::thread;
use std::net::{TcpListener, TcpStream};
use std::io::{Write, BufWriter, BufRead, BufReader};
use std::sync::{Arc,Mutex};
use std::collections::LinkedList;

enum Command<'a> {
    Quit,
    Push(&'a str),
    Pop,
}

fn read_stream(reader: &mut BufReader<&TcpStream>) -> Result<String,()> {
    let mut buffer = Vec::new();
    let result = reader.read_until(b';', &mut buffer);

    if result.is_err() {
        return Err(());
    }

    // EOF
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
    let upcase_cmd: &str = &cmd.to_uppercase();

    match upcase_cmd {
        "PUSH" => {
            if parts.len() == 2 {
                let arg = parts.last().unwrap();
                Ok(Command::Push(arg))
            } else {
                Err("Too many arguments for PUSH".to_string())
            }
        }
        "POP" => Ok(Command::Pop),
        "QUIT" => Ok(Command::Quit),
        _ => Err(format!("Unknown Command: {}", cmd))
    }
}

fn exec_cmd(writer: &mut BufWriter<&TcpStream>, cmd: Result<Command,String>, data: Arc<Mutex<LinkedList<String>>>) -> Result<(),()> {
    match cmd {
        Ok(Command::Push(value)) => {
            let mut data = data.lock().unwrap();
            data.push_back(value.to_string());
            let _ = writer.write(b"SUCCESS");
        }
        Ok(Command::Pop) => {
            let mut data = data.lock().unwrap();
            match data.pop_front() {
                Some(data) => {
                    let _ = writer.write(format!("{}",data).as_bytes());
                }
                None => {
                    let _ = writer.write(b"FAILURE");
                }
            }
        }
        Ok(Command::Quit) => {
            let _ = writer.write(b"Bye bye");
            return Err(());
        }
        Err(message) => {
            let _ = writer.write(message.as_bytes());
        }
    };
    let _ = writer.write(b"\r\n");
    let _ = writer.flush();
    Ok(())
}

fn handle_stream(stream: &TcpStream, data: Arc<Mutex<LinkedList<String>>>) {
    let mut reader = BufReader::new(stream);
    let mut writer = BufWriter::new(stream);
    loop {
        let result = read_stream(&mut reader);
        match result {
            Ok(result) => {
                let cmd = parse_cmd(&result);
                let result = exec_cmd(&mut writer, cmd, data.clone());
                if result.is_err() {
                    break;
                }
            }
            Err(_) => {
                break;
            }
        }
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
                    handle_stream(&stream, data);
                });
            }
            Err(_) => {
            }
        }
    }
}
