use std::io::{Write, BufRead};
use std::time::Duration;
use std::thread;

use queue_table::{QueueName,QueueTable};

use commands::{Command,UncommittedCommand};

const BLOCKING_POP_POLLING_FREQ:u64 = 100;

pub struct Connection<'a> {
    queue_table: &'a QueueTable,
    reader: &'a mut BufRead,
    writer: &'a mut Write,
    uncommitted_cmds: Vec<UncommittedCommand>
}

impl <'a>Connection<'a> {
    pub fn new(reader: &'a mut BufRead, writer: &'a mut Write, queue_table: &'a QueueTable) -> Connection<'a> {
        Connection {
            queue_table: &queue_table,
            reader: reader,
            writer: writer,
            uncommitted_cmds: Vec::new()
        }
    }

    pub fn listen(&mut self) {
        loop {
            let quit = self.process_message();
            self.flush();
            if quit {
                self.rollback();
                break;
            }
        }
    }

    pub fn process_message(&mut self) -> bool {
        match self.read_message() {
            Ok(result) => {
                let cmd = Command::parse(result);
                match cmd {
                    Ok(cmd) => {
                        if self.is_in_transaction() {
                            self.exec_cmd_in_transaction(cmd)
                        } else {
                            self.exec_cmd(cmd)
                        }
                    }
                    Err(message) => {
                        self.write(message.as_bytes());
                        self.write(b"\r\n");
                        false
                    }
                }
            }
            Err(_) => {
                true
            }
        }
    }

    fn is_in_transaction(&self) -> bool {
        self.uncommitted_cmds.len() > 0
    }

    fn write(&mut self, buf: &[u8]) {
        let _ = self.writer.write(buf);
    }

    fn flush(&mut self) {
        let _ = self.writer.flush();
    }

    fn read_message(&mut self) -> Result<Vec<u8>,()> {
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
                        self.write(format!("{}\r\n", data).as_bytes());
                        Ok(data)
                    }
                    None => {
                        self.write(b"NO DATA\r\n");
                        Err(())
                    }
                }
            }
            None => {
                self.write(b"NO SUCH QUEUE\r\n");
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

    fn exec_cmd(&mut self, cmd: Command) -> bool {
        match cmd {
            Command::Push(value, queue_name) => {
                exec_push(value, &self.queue_table, queue_name);
                self.write(b"SUCCESS\r\n");
            }
            Command::Pop(queue_name) => {
                let _ = self.exec_pop(queue_name);
            }
            Command::BlockingPop(queue_name) => {
                let data = self.exec_blocking_pop(queue_name.clone());
                self.write(format!("{}\r\n", data).as_bytes());
            }
            Command::Quit => {
                self.write(b"Bye bye");
                return true;
            }
            Command::Begin => {
                self.uncommitted_cmds.push(UncommittedCommand::Begin);
            }
            Command::Abort => {
                self.write(b"Not in transaction\r\n");
            }
            Command::Commit => {
                self.write(b"Not in transaction\r\n");
            }
        };
        false
    }

    fn exec_cmd_in_transaction(&mut self, cmd: Command) -> bool {
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
                self.write(format!("{}\r\n", data).as_bytes());
                self.uncommitted_cmds.push(UncommittedCommand::Pop(data, queue_name));
            }
            Command::Quit => {
                self.write(b"Bye bye");
                return true;
            }
            Command::Begin => {
                self.write(b"Already in transaction\r\n");
            }
            Command::Abort => {
                self.rollback();
            }
            Command::Commit => {
                self.commit();
            }
        };
        false
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
