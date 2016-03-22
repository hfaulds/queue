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

struct CommandToken {
    command: String,
    arguments: Vec<String>
}

impl Command  {
    pub fn parse(buffer: Vec<u8>) -> ParseResult {
        let result = Command::tokenize(buffer);
        match result {
            Ok(command_token) => {
                match &command_token.command as &str {
                    "PUSH" => {
                        parse_push(command_token.arguments)
                    }
                    "POP" => {
                        parse_pop(command_token.arguments)
                    }
                    "BPOP" => {
                        parse_bpop(command_token.arguments)
                    }
                    "QUIT" => Ok(Command::Quit),
                    "BEGIN" => Ok(Command::Begin),
                    "COMMIT" => Ok(Command::Commit),
                    "ABORT" => Ok(Command::Abort),
                    cmd => Err(format!("Unknown Command: {}", cmd))
                }
            }
            Err(e) => {
                Err(e)
            }
        }
    }

    fn tokenize(buffer: Vec<u8>) -> Result<CommandToken, String> {
        let result = String::from_utf8(buffer);

        if result.is_err() {
            return Err("Command included non utf8 characters".to_string());
        }

        let mut command = String::new();
        let mut arguments = Vec::new();
        let mut current_string = String::new();
        let mut is_escaped = false;
        let mut is_quoted = false;
        let mut is_parsing_command = true;

        let buffer = result.unwrap();
        for c in buffer.trim().chars() {
            if is_parsing_command {
                match c {
                    '\'' => {
                        return Err("Unexpected quote in command name".to_string());
                    },
                    '\\' => {
                        return Err("Unexpected backslash in command name".to_string());
                    },
                    ' ' => {
                        is_parsing_command = false;
                    },
                    _ => {
                        command.push(c);
                    }
                }
            } else if !is_quoted {
                match c {
                    '\'' => { is_quoted = true },
                    ' ' => {},
                    _ => {
                        return Err(format!("unqouted character: {}", c));
                    },
                }
            } else if is_escaped {
                match c {
                    '\'' => {
                        current_string.push(c);
                    },
                    '\\' => {
                        current_string.push(c);
                    },
                    _ => {
                        return Err(format!("unescapeable character: {}", c));
                    }
                }
                is_escaped = false;
            } else {
                match c {
                    '\'' => {
                        arguments.push(current_string);
                        is_quoted = false;
                        current_string = String::new();
                    },
                    '\\' => {
                        is_escaped = true;
                    },
                    _ => {
                        current_string.push(c);
                    }
                }
            }
        }

        if current_string.len() == 0 {
            Ok(CommandToken { command: command.to_uppercase(), arguments: arguments })
        } else {
            Err("trailing charactrs".to_string())
        }
    }
}

fn parse_push(arguments: Vec<String>) -> ParseResult {
    if arguments.len() == 2 {
        let queue_name = arguments[0].clone();
        let data = arguments[1].clone();
        Ok(Command::Push(data, queue_name))
    } else {
        Err("Incorrect number of arguments for PUSH".to_string())
    }
}


fn parse_pop(arguments: Vec<String>) -> ParseResult {
    if arguments.len() == 1 {
        let queue_name = arguments[0].clone();
        Ok(Command::Pop(queue_name))
    } else {
        Err("Incorrect number of arguments for PUSH".to_string())
    }
}

fn parse_bpop(arguments: Vec<String>) -> ParseResult {
    if arguments.len() == 1 {
        let queue_name = arguments[0].clone();
        Ok(Command::BlockingPop(queue_name))
    } else {
        Err("Incorrect number of arguments for PUSH".to_string())
    }
}
