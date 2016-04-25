use queue_table::{QueueName};
use parse_commands::{parse_command, ParseResult};

#[derive(PartialEq)]
#[derive(Debug)]
pub enum Command {
    Quit,
    Push(String, QueueName),
    Pop(QueueName),
    BlockingPop(QueueName),
    Begin,
    Commit,
    Abort
}

pub enum UncommittedCommand {
    Begin,
    Push(String, QueueName),
    Pop(String, QueueName),
}

impl Command  {
    pub fn parse(buffer: Vec<u8>) -> ParseResult {
        parse_command(buffer)
    }
}
