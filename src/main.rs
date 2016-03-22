use std::thread;
use std::net::{TcpListener, TcpStream};
use std::io::{Write, BufWriter, BufRead, BufReader};
use std::time::Duration;

mod queue_table;
use queue_table::{QueueName,QueueTable};

mod commands;
use commands::{Command,UncommittedCommand};

const BLOCKING_POP_POLLING_FREQ:u64 = 100;

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

fn exec_pop(writer: &mut BufWriter<&TcpStream>, queue_table: QueueTable, queue_name: QueueName) -> Result<(String),()> {
    match queue_table.get_queue(&queue_name) {
        Some(queue)  => {
            let mut queue = queue.lock().unwrap();
            match queue.pop_front() {
                Some(data) => {
                    let _ = writer.write(format!("{}\r\n", data).as_bytes());
                    Ok(data)
                }
                None => {
                    let _ = writer.write(b"NO DATA\r\n");
                    Err(())
                }
            }
        }
        None => {
            let _ = writer.write(b"NO SUCH QUEUE\r\n");
            Err(())
        }
    }
}


fn exec_blocking_pop(queue_table: QueueTable, queue_name: QueueName) -> String {
    let queue = queue_table.get_or_create_queue(queue_name);
    loop {
        let mut queue = queue.lock().unwrap();
        match queue.pop_front() {
            Some(data) => {
                return data;
            }
            None => {
            }
        }
        std::thread::sleep(Duration::from_millis(BLOCKING_POP_POLLING_FREQ));
    }
}

fn exec_push(value: String, queue_table: QueueTable, queue_name: QueueName) {
    let queue = queue_table.get_or_create_queue(queue_name);
    let mut queue = queue.lock().unwrap();
    queue.push_back(value.to_string());
}

fn exec_cmd(
    writer: &mut BufWriter<&TcpStream>,
    cmd: Command,
    uncommitted_cmds: &mut Vec<UncommittedCommand>,
    queue_table: QueueTable
    ) -> Result<(),()> {

    match cmd {
        Command::Push(value, queue_name) => {
            exec_push(value, queue_table, queue_name);
            let _ = writer.write(b"SUCCESS\r\n");
        }
        Command::Pop(queue_name) => {
            let _ = exec_pop(writer, queue_table, queue_name);
        }
        Command::BlockingPop(queue_name) => {
            let queue_table = exec_blocking_pop(queue_table, queue_name);
            let _ = writer.write(format!("{}",queue_table).as_bytes());
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

fn exec_cmd_in_transaction(
    writer: &mut BufWriter<&TcpStream>,
    cmd: Command,
    uncommitted_cmds: &mut Vec<UncommittedCommand>,
    queue_table: QueueTable
    ) -> Result<(),()> {

    match cmd {
        Command::Push(value, queue_name) => {
            uncommitted_cmds.push(UncommittedCommand::Push(value, queue_name));
        }
        Command::Pop(queue_name) => {
            let data = exec_pop(writer, queue_table, queue_name.clone());
            if data.is_ok() {
                uncommitted_cmds.push(UncommittedCommand::Pop(data.unwrap(), queue_name));
            }
        }
        Command::BlockingPop(queue_name) => {
            let data = exec_blocking_pop(queue_table, queue_name.clone());
            let _ = writer.write(format!("{}",data).as_bytes());
            uncommitted_cmds.push(UncommittedCommand::Pop(data, queue_name));
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
            rollback(uncommitted_cmds, queue_table);
        }
        Command::Commit => {
            commit(uncommitted_cmds, queue_table);
        }
    };
    let _ = writer.flush();
    Ok(())
}

fn rollback(uncommitted_cmds: &mut Vec<UncommittedCommand>, queue_table: QueueTable) {
    for cmd in uncommitted_cmds.drain(..) {
        match cmd {
            UncommittedCommand::Pop(value, queue_name) => {
                exec_push(value, queue_table.clone(), queue_name);
            },
            _ => {
            }
        }
    }
}

fn commit(uncommitted_cmds: &mut Vec<UncommittedCommand>, queue_table: QueueTable) {
    for cmd in uncommitted_cmds.drain(..) {
        match cmd {
            UncommittedCommand::Push(value, queue_name) => {
                exec_push(value, queue_table.clone(), queue_name);
            }
            _ => {
            }
        }
    }
}

fn handle_stream(stream: &TcpStream, queue_table: QueueTable) {
    let mut reader = BufReader::new(stream);
    let mut writer = BufWriter::new(stream);

    let mut uncommitted_cmds: Vec<UncommittedCommand> = Vec::new();

    loop {
        let result = read_stream(&mut reader);
        match result {
            Ok(result) => {
                let cmd = Command::parse(result);
                match cmd {
                    Ok(cmd) => {
                        let in_transaction = uncommitted_cmds.len();
                        let result = if in_transaction > 0 {
                            exec_cmd_in_transaction(&mut writer, cmd, &mut uncommitted_cmds, queue_table.clone())
                        } else {
                            exec_cmd(&mut writer, cmd, &mut uncommitted_cmds, queue_table.clone())
                        };
                        if result.is_err() {
                            rollback(&mut uncommitted_cmds, queue_table);
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
                rollback(&mut uncommitted_cmds, queue_table);
                break;
            }
        }
    }
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:5248").unwrap();

    let queue_table = QueueTable::new();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let queue_table = queue_table.clone();
                thread::spawn(move|| {
                    handle_stream(&stream, queue_table);
                });
            }
            Err(_) => {
            }
        }
    }
}
