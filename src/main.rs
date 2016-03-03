use std::thread;
use std::net::{TcpListener, TcpStream};
use std::io::{Write, BufWriter, BufRead, BufReader};
use std::sync::{Arc,Mutex};
use std::collections::LinkedList;
use std::time::Duration;

const BLOCKING_POP_POLLING_FREQ:u64 = 100;

enum Command {
    Quit,
    Push(String),
    Pop,
    BlockingPop,
    Begin,
    Commit,
    Abort
}

enum UncommittedCommand {
    Begin,
    Push(String),
    Pop(String),
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
                let arg = (*parts.last().unwrap()).to_string();
                Ok(Command::Push(arg))
            } else {
                Err("Too many arguments for PUSH".to_string())
            }
        }
        "POP" => Ok(Command::Pop),
        "BPOP" => Ok(Command::BlockingPop),
        "QUIT" => Ok(Command::Quit),
        "BEGIN" => Ok(Command::Begin),
        "COMMIT" => Ok(Command::Commit),
        "ABORT" => Ok(Command::Abort),
        _ => Err(format!("Unknown Command: {}", cmd))
    }
}

fn exec_blocking_pop(data: Arc<Mutex<LinkedList<String>>>) -> String {
    loop {
        let mut data = data.lock().unwrap();
        match data.pop_front() {
            Some(data) => {
                return data;
            }
            None => {
            }
        }
        std::thread::sleep(Duration::from_millis(BLOCKING_POP_POLLING_FREQ));
    }
}

fn exec_push(value: String, data: Arc<Mutex<LinkedList<String>>>) {
    let mut data = data.lock().unwrap();
    data.push_back(value.to_string());
}

fn exec_cmd(
    writer: &mut BufWriter<&TcpStream>,
    cmd: Command,
    uncommitted_cmds: &mut Vec<UncommittedCommand>,
    data: Arc<Mutex<LinkedList<String>>>
    ) -> Result<(),()> {

    let in_transaction = uncommitted_cmds.len() > 0;

    match cmd {
        Command::Push(value) => {
            if in_transaction {
                uncommitted_cmds.push(UncommittedCommand::Push(value));
            } else {
                exec_push(value, data);
                let _ = writer.write(b"SUCCESS");
            }
        }
        Command::Pop => {
            let mut data = data.lock().unwrap();
            match data.pop_front() {
                Some(data) => {
                    let _ = writer.write(format!("{}",data).as_bytes());
                    if in_transaction {
                        uncommitted_cmds.push(UncommittedCommand::Pop(data));
                    }
                }
                None => {
                    let _ = writer.write(b"FAILURE");
                }
            }
        }
        Command::BlockingPop => {
            let data = exec_blocking_pop(data);
            let _ = writer.write(format!("{}",data).as_bytes());
            if in_transaction {
                uncommitted_cmds.push(UncommittedCommand::Pop(data));
            }
        }
        Command::Quit => {
            let _ = writer.write(b"Bye bye");
            let _ = writer.flush();
            return Err(());
        }
        Command::Begin => {
            if in_transaction {
                let _ = writer.write(b"Already in transaction");
            } else {
                uncommitted_cmds.push(UncommittedCommand::Begin);
                return Ok(());
            }
        }
        Command::Abort => {
            if in_transaction {
                rollback(uncommitted_cmds, data);
                return Ok(());
            } else {
                let _ = writer.write(b"Already in transaction");
            }
        }
        Command::Commit => {
            for cmd in uncommitted_cmds.drain(1..) {
                match cmd {
                    UncommittedCommand::Push(value) => {
                        exec_push(value, data.clone());
                    }
                    _ => {
                    }
                }
            }
        }
    };

    let _ = writer.write(b"\r\n");
    let _ = writer.flush();
    Ok(())
}

fn rollback(uncommitted_cmds: &mut Vec<UncommittedCommand>, data: Arc<Mutex<LinkedList<String>>>) {
    for cmd in uncommitted_cmds.drain(1..) {
        match cmd {
            UncommittedCommand::Pop(value) => {
                exec_push(value, data.clone());
            },
            _ => {
            }
        }
    }
}

fn handle_stream(stream: &TcpStream, data: Arc<Mutex<LinkedList<String>>>) {
    let mut reader = BufReader::new(stream);
    let mut writer = BufWriter::new(stream);

    let mut uncommitted_cmds: Vec<UncommittedCommand > = Vec::new();

    loop {
        let result = read_stream(&mut reader);
        match result {
            Ok(result) => {
                let cmd = parse_cmd(&result);
                match cmd {
                    Ok(cmd) => {
                        let result = exec_cmd(&mut writer, cmd, &mut uncommitted_cmds, data.clone());
                        if result.is_err() {
                            rollback(&mut uncommitted_cmds, data);
                            break;
                        }
                    }
                    Err(message) => {
                        let _ = writer.write(message.as_bytes());
                    }
                }
            }
            Err(_) => {
                rollback(&mut uncommitted_cmds, data);
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
