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
        let result = Command::tokenize(buffer);
        match result {
            Ok(tokens) => {
                let cmd: &str = tokens.first().unwrap();
                let upcase_cmd: &str = &cmd.to_uppercase();

                match upcase_cmd {
                    "PUSH" => {
                        parse_push(&tokens)
                    }
                    "POP" => {
                        parse_pop(&tokens)
                    }
                    "BPOP" => {
                        parse_bpop(&tokens)
                    }
                    "QUIT" => Ok(Command::Quit),
                    "BEGIN" => Ok(Command::Begin),
                    "COMMIT" => Ok(Command::Commit),
                    "ABORT" => Ok(Command::Abort),
                    _ => Err(format!("Unknown Command: {}", cmd))
                }
            }
            Err(e) => {
                Err(e)
            }
        }
    }

    fn tokenize(buffer: Vec<u8>) -> Result<Vec<String>, String> {
        let result = String::from_utf8(buffer);

        if result.is_err() {
            return Err("Command included non utf8 characters".to_string());
        }

        let mut tokens = Vec::new();
        let mut current_string = String::new();
        let mut is_escaped = false;
        let mut is_quoted = false;

        for c in result.unwrap().trim().chars() {
            if !is_quoted {
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
                        tokens.push(current_string);
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
            Ok(tokens)
        } else {
            Err("trailing charactrs".to_string())
        }
    }
}

fn parse_push(tokens: &Vec<String>) -> ParseResult {
    if tokens.len() == 3 {
        let queue_name = tokens[1].clone();
        let data = tokens[2].clone();
        Ok(Command::Push(data, queue_name))
    } else {
        Err("Incorrect number of arguments for PUSH".to_string())
    }
}


fn parse_pop(tokens: &Vec<String>) -> ParseResult {
    if tokens.len() == 2 {
        let queue_name = tokens[1].clone();
        Ok(Command::Pop(queue_name))
    } else {
        Err("Incorrect number of arguments for PUSH".to_string())
    }
}

fn parse_bpop(tokens: &Vec<String>) -> ParseResult {
    if tokens.len() == 2 {
        let queue_name = tokens[1].clone();
        Ok(Command::BlockingPop(queue_name))
    } else {
        Err("Incorrect number of arguments for PUSH".to_string())
    }
}
