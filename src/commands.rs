use queue_table::{QueueName};

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

pub type ParseResult = Result<Command,String>;

impl Command  {
    pub fn parse(buffer: Vec<u8>) -> ParseResult {
        let result = String::from_utf8(buffer);
        match result {
            Ok(buffer) => {
                let parts: Vec<&str> = buffer.trim().split(" ").collect();
                let cmd: &str = parts.first().unwrap();
                let upcase_cmd: &str = &cmd.to_uppercase();

                match upcase_cmd {
                    "PUSH" => {
                        parse_push(parts)
                    }
                    "POP" => {
                        parse_pop(parts)
                    }
                    "BPOP" => {
                        parse_bpop(parts)
                    }
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
}

fn parse_push(parts: Vec<&str>) -> ParseResult {
    if parts.len() == 3 {
        let queue_name = (*parts[1]).to_string();
        let data = (*parts.last().unwrap()).to_string();
        Ok(Command::Push(data, queue_name))
    } else {
        Err("Incorrect number of arguments for PUSH".to_string())
    }
}


fn parse_pop(parts: Vec<&str>) -> ParseResult {
    if parts.len() == 2 {
        let queue_name = (*parts.last().unwrap()).to_string();
        Ok(Command::Pop(queue_name))
    } else {
        Err("Incorrect number of arguments for PUSH".to_string())
    }
}

fn parse_bpop(parts: Vec<&str>) -> ParseResult {
    if parts.len() == 2 {
        let queue_name = (*parts.last().unwrap()).to_string();
        Ok(Command::BlockingPop(queue_name))
    } else {
        Err("Incorrect number of arguments for PUSH".to_string())
    }
}
