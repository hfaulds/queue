use std::io::{Write};
use std::thread;
use std::time::Duration;

use queue_table::{QueueName,QueueTable};
use commands::{Command,UncommittedCommand};

pub enum CommandResult  {
    Success,
    Data(String),
    Error(String),
    Disconnect,
}

impl CommandResult {
    pub fn write(self, writer: &mut Write) {
        match self {
            CommandResult::Success => { },
            CommandResult::Data(message) | CommandResult::Error(message) => {
                let _ = writer.write(format!("{}\r\n", message).as_bytes());
            },
            CommandResult::Disconnect => {
                let _ = writer.write(b"Bye Bye\r\n");
            }
        }
    }

    pub fn command_result(&self) -> TransactionResult {
        match *self {
            CommandResult::Disconnect => {
                TransactionResult::Quit
            }
            _ => {
                TransactionResult::Continue
            },
        }
    }
}

pub enum TransactionResult {
    Continue,
    Quit,
}

impl TransactionResult {
    pub fn is_quit(&self) -> bool {
        match *self {
            TransactionResult::Continue => {
                false
            },
            TransactionResult::Quit => {
                true
            }
        }
    }
}

const BLOCKING_POP_POLLING_FREQ:u64 = 100;

pub struct Transaction {
    uncommitted_cmds: Vec<UncommittedCommand>
}

impl Transaction {
    pub fn new() -> Transaction {
        Transaction{ uncommitted_cmds: Vec::new() }
    }

    pub fn exec(&mut self, cmd: Command, queue_table: &QueueTable) -> CommandResult {
        if self.uncommitted_cmds.len() > 0 {
            self.stage_cmd(cmd, queue_table)
        } else {
            self.exec_cmd(cmd, queue_table)
        }
    }

    fn exec_cmd(&mut self, cmd: Command, queue_table: &QueueTable) -> CommandResult {
        match cmd {
            Command::Push(value, queue_name) => {
                exec_push(value, queue_table, queue_name);
                CommandResult::Data("SUCCESS".to_string())
            }
            Command::Pop(queue_name) => {
                exec_pop(queue_table, queue_name)
            }
            Command::BlockingPop(queue_name) => {
                let data = exec_blocking_pop(queue_table, queue_name);
                CommandResult::Data( data)
            }
            Command::Quit => {
                CommandResult::Disconnect
            }
            Command::Begin => {
                self.uncommitted_cmds.push(UncommittedCommand::Begin);
                CommandResult::Success
            }
            Command::Abort => {
                CommandResult::Error("Not in transaction\r\n".to_string())
            }
            Command::Commit => {
                CommandResult::Error("Not in transaction\r\n".to_string())
            }
        }
    }

    fn stage_cmd(&mut self, cmd: Command, queue_table: &QueueTable) -> CommandResult {
        match cmd {
            Command::Push(value, queue_name) => {
                self.uncommitted_cmds.push(UncommittedCommand::Push(value, queue_name));
                CommandResult::Success
            }
            Command::Pop(queue_name) => {
                let result = exec_pop(queue_table, queue_name.clone());
                match result {
                    CommandResult::Data(data) => {
                        self.uncommitted_cmds.push(UncommittedCommand::Pop(data.clone(), queue_name));
                        CommandResult::Data(data)
                    }
                    result => {
                        result
                    }
                }
            }
            Command::BlockingPop(queue_name) => {
                let data = exec_blocking_pop(queue_table, queue_name.clone());
                self.uncommitted_cmds.push(UncommittedCommand::Pop(data.clone(), queue_name));
                CommandResult::Data(data)
            }
            Command::Quit => {
                CommandResult::Disconnect
            }
            Command::Begin => {
                CommandResult::Error("Already in transaction\r\n".to_string())
            }
            Command::Abort => {
                self.rollback(queue_table);
                CommandResult::Success
            }
            Command::Commit => {
                self.commit(queue_table);
                CommandResult::Success
            }
        }
    }

    pub fn rollback(&mut self, queue_table: &QueueTable) {
        for cmd in self.uncommitted_cmds.drain(..) {
            match cmd {
                UncommittedCommand::Pop(value, queue_name) => {
                    exec_push(value, queue_table, queue_name);
                },
                _ => {
                }
            }
        }
    }

    fn commit(&mut self, queue_table: &QueueTable) {
        for cmd in self.uncommitted_cmds.drain(..) {
            match cmd {
                UncommittedCommand::Push(value, queue_name) => {
                    exec_push(value, queue_table, queue_name);
                }
                _ => {
                }
            }
        }
    }
}

fn exec_push(value: String, queue_table: &QueueTable, queue_name: QueueName) {
    let queue = queue_table.get_or_create_queue(queue_name);
    queue.push_back(value.to_string());
}

fn exec_pop(queue_table: &QueueTable, queue_name: QueueName) -> CommandResult {
    match queue_table.get_queue(&queue_name) {
        Some(queue)  => {
            match queue.pop_front() {
                Some(data) => {
                    CommandResult::Data(data)
                }
                None => {
                    CommandResult::Error("NO DATA\r\n".to_string())
                }
            }
        }
        None => {
            CommandResult::Error("NO SUCH QUEUE\r\n".to_string())
        }
    }
}

fn exec_blocking_pop(queue_table: &QueueTable, queue_name: QueueName) -> String {
    let queue = queue_table.get_or_create_queue(queue_name);
    loop {
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
