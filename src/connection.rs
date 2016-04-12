use std::net::{TcpStream};
use std::io::{Write, BufWriter, BufRead, BufReader};
use std::time::Duration;
use std::thread;

use queue_table::{QueueName,QueueTable};

use commands::{Command,UncommittedCommand};

const BLOCKING_POP_POLLING_FREQ:u64 = 100;

pub struct Connection<'a> {
    queue_table: &'a QueueTable,
    reader: BufReader<& 'a TcpStream>,
    writer: BufWriter<& 'a TcpStream>,
    uncommitted_cmds: Vec<UncommittedCommand>
}

impl <'a>Connection<'a> {
    pub fn new(stream: &'a TcpStream, queue_table: &'a QueueTable) -> Connection<'a> {
        Connection {
            queue_table: &queue_table,
            reader: BufReader::new(&stream),
            writer: BufWriter::new(&stream),
            uncommitted_cmds: Vec::new()
        }
    }

    pub fn listen(&mut self) {
        loop {
            let result = self.read_stream();
            match result {
                Ok(result) => {
                    let cmd = Command::parse(result);
                    match cmd {
                        Ok(cmd) => {
                            let in_transaction = self.uncommitted_cmds.len();
                            let result = if in_transaction > 0 {
                                self.exec_cmd_in_transaction(cmd)
                            } else {
                                self.exec_cmd(cmd)
                            };
                            if result.is_err() {
                                self.rollback();
                                let _ = self.writer.flush();
                                break;
                            }
                        }
                        Err(message) => {
                            let _ = self.writer.write(message.as_bytes());
                            let _ = self.writer.write(b"\r\n");
                        }
                    }
                }
                Err(_) => {
                    self.rollback();
                    let _ = self.writer.flush();
                    break;
                }
            }
            let _ = self.writer.flush();
        }
    }

    fn read_stream(&mut self) -> Result<Vec<u8>,()> {
        let mut buffer = Vec::new();
        let result = self.reader.read_until(b';', &mut buffer);

        if result.is_err() {
            return Err(());
        }

        // EOF
        if buffer.pop() != Some(b';') {
            return Err(());
        }

        return Ok(buffer);
    }

    fn exec_pop(&mut self, queue_name: QueueName) -> Result<(String),()> {
        match self.queue_table.get_queue(&queue_name) {
            Some(queue)  => {
                let mut queue = queue.lock().unwrap();
                match queue.pop_front() {
                    Some(data) => {
                        let _ = self.writer.write(format!("{}\r\n", data).as_bytes());
                        Ok(data)
                    }
                    None => {
                        let _ = self.writer.write(b"NO DATA\r\n");
                        Err(())
                    }
                }
            }
            None => {
                let _ = self.writer.write(b"NO SUCH QUEUE\r\n");
                Err(())
            }
        }
    }

    fn exec_blocking_pop(&mut self, queue_name: QueueName) -> String {
        let queue = self.queue_table.get_or_create_queue(queue_name);
        loop {
            let mut queue = queue.lock().unwrap();
            match queue.pop_front() {
                Some(data) => {
                    return data;
                }
                None => {
                }
            }
            thread::sleep(Duration::from_millis(BLOCKING_POP_POLLING_FREQ));
        }
    }

    fn exec_cmd(&mut self, cmd: Command) -> Result<(),()> {
        match cmd {
            Command::Push(value, queue_name) => {
                exec_push(value, &self.queue_table, queue_name);
                let _ = self.writer.write(b"SUCCESS\r\n");
            }
            Command::Pop(queue_name) => {
                let _ = self.exec_pop(queue_name);
            }
            Command::BlockingPop(queue_name) => {
                let data = self.exec_blocking_pop(queue_name.clone());
                let _ = self.writer.write(format!("{}\r\n", data).as_bytes());
            }
            Command::Quit => {
                let _ = self.writer.write(b"Bye bye");
                return Err(());
            }
            Command::Begin => {
                self.uncommitted_cmds.push(UncommittedCommand::Begin);
            }
            Command::Abort => {
                let _ = self.writer.write(b"Not in transaction\r\n");
            }
            Command::Commit => {
                let _ = self.writer.write(b"Not in transaction\r\n");
            }
        };

        Ok(())
    }

    fn exec_cmd_in_transaction(&mut self, cmd: Command) -> Result<(),()> {
        match cmd {
            Command::Push(value, queue_name) => {
                self.uncommitted_cmds.push(UncommittedCommand::Push(value, queue_name));
            }
            Command::Pop(queue_name) => {
                let data = self.exec_pop(queue_name.clone());
                if data.is_ok() {
                    self.uncommitted_cmds.push(UncommittedCommand::Pop(data.unwrap(), queue_name));
                }
            }
            Command::BlockingPop(queue_name) => {
                let data = self.exec_blocking_pop(queue_name.clone());
                let _ = self.writer.write(format!("{}\r\n", data).as_bytes());
                self.uncommitted_cmds.push(UncommittedCommand::Pop(data, queue_name));
            }
            Command::Quit => {
                let _ = self.writer.write(b"Bye bye");
                return Err(());
            }
            Command::Begin => {
                let _ = self.writer.write(b"Already in transaction\r\n");
            }
            Command::Abort => {
                self.rollback();
            }
            Command::Commit => {
                self.commit();
            }
        };
        Ok(())
    }

    fn rollback(&mut self) {
        for cmd in self.uncommitted_cmds.drain(..) {
            match cmd {
                UncommittedCommand::Pop(value, queue_name) => {
                    exec_push(value, &self.queue_table, queue_name);
                },
                _ => {
                }
            }
        }
    }

    fn commit(&mut self) {
        for cmd in self.uncommitted_cmds.drain(..) {
            match cmd {
                UncommittedCommand::Push(value, queue_name) => {
                    exec_push(value, &self.queue_table, queue_name);
                }
                _ => {
                }
            }
        }
    }
}

// exec_push can't be moved onto Connection
// commit and rollback borrow self as mutable once already
fn exec_push(value: String, queue_table: &QueueTable, queue_name: QueueName) {
    let queue = queue_table.get_or_create_queue(queue_name);
    let mut queue = queue.lock().unwrap();
    queue.push_back(value.to_string());
}