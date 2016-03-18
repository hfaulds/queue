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

pub fn parse_cmd(buffer: Vec<u8>) -> Result<Command,String> {
    let result = String::from_utf8(buffer);
    match result {
        Ok(buffer) => {
            let parts: Vec<&str> = buffer.trim().split(" ").collect();
            let cmd: &str = parts.first().unwrap();
            let upcase_cmd: &str = &cmd.to_uppercase();

            match upcase_cmd {
                "PUSH" => {
                    if parts.len() == 3 {
                        let queue_name = (*parts[1]).to_string();
                        let data = (*parts.last().unwrap()).to_string();
                        Ok(Command::Push(data, queue_name))
                    } else {
                        Err("Incorrect number of arguments for PUSH".to_string())
                    }
                }
                "POP" => {
                    if parts.len() == 2 {
                        let queue_name = (*parts.last().unwrap()).to_string();
                        Ok(Command::Pop(queue_name))
                    } else {
                        Err("Incorrect number of arguments for PUSH".to_string())
                    }
                }
                "BPOP" => {
                    if parts.len() == 2 {
                        let queue_name = (*parts.last().unwrap()).to_string();
                        Ok(Command::BlockingPop(queue_name))
                    } else {
                        Err("Incorrect number of arguments for PUSH".to_string())
                    }
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

