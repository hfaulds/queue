use std::thread;
use std::net::{TcpListener, TcpStream};
use std::io::{Write, BufWriter, BufRead, BufReader};
use std::sync::{Arc,Mutex};
use std::time::Duration;

const BLOCKING_POP_POLLING_FREQ:u64 = 100;

type Queue = Arc<Mutex<Vec<String>>>;

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

fn read_stream(reader: &mut BufReader<&TcpStream>) -> Result<Vec<u8>,()> {
    let mut buffer = Vec::new();
    let result = reader.read_until(b';', &mut buffer);

    if result.is_err() {
        return Err(());
    }

    // EOF
    if buffer.pop() != Some(b';') {
        return Err(());
    }

    return Ok(buffer);
}

fn parse_cmd(buffer: Vec<u8>) -> Result<Command,String> {
    let result = String::from_utf8(buffer);
    match result {
        Ok(buffer) => {
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
        Err(_) => {
            return Err("Command included non utf8 characters".to_string());
        }
    }
}

fn exec_blocking_pop(data: Queue) -> String {
    loop {
        let mut data = data.lock().unwrap();
        match data.pop() {
            Some(data) => {
                return data;
            }
            None => {
            }
        }
        std::thread::sleep(Duration::from_millis(BLOCKING_POP_POLLING_FREQ));
    }
}

fn exec_push(value: String, data: Queue) {
    let mut data = data.lock().unwrap();
    data.push(value.to_string());
}

fn exec_cmd(
    writer: &mut BufWriter<&TcpStream>,
    cmd: Command,
    uncommitted_cmds: &mut Vec<UncommittedCommand>,
    data: Queue
    ) -> Result<(),()> {

    match cmd {
        Command::Push(value) => {
            exec_push(value, data);
            let _ = writer.write(b"SUCCESS\r\n");
        }
        Command::Pop => {
            let _ = exec_pop(writer, data);
        }
        Command::BlockingPop => {
            let data = exec_blocking_pop(data);
            let _ = writer.write(format!("{}",data).as_bytes());
        }
        Command::Quit => {
            let _ = writer.write(b"Bye bye");
            let _ = writer.flush();
            return Err(());
        }
        Command::Begin => {
            uncommitted_cmds.push(UncommittedCommand::Begin);
        }
        Command::Abort => {
            let _ = writer.write(b"Not in transaction\r\n");
        }
        Command::Commit => {
            let _ = writer.write(b"Not in transaction\r\n");
        }
    };

    let _ = writer.flush();
    Ok(())
}

fn exec_pop(writer: &mut BufWriter<&TcpStream>, data: Queue) -> Result<(String),()> {
    let mut data = data.lock().unwrap();
    match data.pop() {
        Some(data) => {
            let _ = writer.write(format!("{}\r\n", data).as_bytes());
            Ok(data)
        }
        None => {
            let _ = writer.write(b"FAILURE\r\n");
            Err(())
        }
    }
}

fn exec_cmd_in_transaction(
    writer: &mut BufWriter<&TcpStream>,
    cmd: Command,
    uncommitted_cmds: &mut Vec<UncommittedCommand>,
    data: Queue
    ) -> Result<(),()> {

    match cmd {
        Command::Push(value) => {
            uncommitted_cmds.push(UncommittedCommand::Push(value));
        }
        Command::Pop => {
            let result = exec_pop(writer, data);
            if result.is_ok() {
                uncommitted_cmds.push(UncommittedCommand::Pop(result.unwrap()));
            }
        }
        Command::BlockingPop => {
            let data = exec_blocking_pop(data);
            let _ = writer.write(format!("{}",data).as_bytes());
            uncommitted_cmds.push(UncommittedCommand::Pop(data));
        }
        Command::Quit => {
            let _ = writer.write(b"Bye bye");
            let _ = writer.flush();
            return Err(());
        }
        Command::Begin => {
            let _ = writer.write(b"Already in transaction\r\n");
        }
        Command::Abort => {
            rollback(uncommitted_cmds, data);
        }
        Command::Commit => {
            commit(uncommitted_cmds, data);
        }
    };
    let _ = writer.flush();
    Ok(())
}

fn rollback(uncommitted_cmds: &mut Vec<UncommittedCommand>, data: Queue) {
    for cmd in uncommitted_cmds.drain(..) {
        match cmd {
            UncommittedCommand::Pop(value) => {
                exec_push(value, data.clone());
            },
            _ => {
            }
        }
    }
}

fn commit(uncommitted_cmds: &mut Vec<UncommittedCommand>, data: Queue) {
    for cmd in uncommitted_cmds.drain(..) {
        match cmd {
            UncommittedCommand::Push(value) => {
                exec_push(value, data.clone());
            }
            _ => {
            }
        }
    }
}

fn handle_stream(stream: &TcpStream, queue: Queue) {
    let mut reader = BufReader::new(stream);
    let mut writer = BufWriter::new(stream);

    let mut uncommitted_cmds: Vec<UncommittedCommand> = Vec::new();

    loop {
        let result = read_stream(&mut reader);
        match result {
            Ok(result) => {
                let cmd = parse_cmd(result);
                match cmd {
                    Ok(cmd) => {
                        let in_transaction = uncommitted_cmds.len();
                        let result = if in_transaction > 0 {
                            exec_cmd_in_transaction(&mut writer, cmd, &mut uncommitted_cmds, queue.clone())
                        } else {
                            exec_cmd(&mut writer, cmd, &mut uncommitted_cmds, queue.clone())
                        };
                        if result.is_err() {
                            rollback(&mut uncommitted_cmds, queue);
                            break;
                        }
                    }
                    Err(message) => {
                        let _ = writer.write(message.as_bytes());
                        let _ = writer.write(b"\r\n");
                        let _ = writer.flush();
                    }
                }
            }
            Err(_) => {
                rollback(&mut uncommitted_cmds, queue);
                break;
            }
        }
    }
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:5248").unwrap();

    let queue = Arc::new(Mutex::new(Vec::new()));

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let queue = queue.clone();
                thread::spawn(move|| {
                    handle_stream(&stream, queue);
                });
            }
            Err(_) => {
            }
        }
    }
}
