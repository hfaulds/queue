use commands::{Command};
use std::str::{Chars};

pub type ParseResult = Result<Command,String>;

pub fn parse_command(buffer: Vec<u8>) -> ParseResult {
    let buffer = try!(string_from_utf8(buffer));
    let mut chars = buffer.chars();
    let command_name = try!(parse_command_name(&mut chars));
    let arguments = try!(parse_arguments(&mut chars));
    build_command(command_name, arguments)
}

fn string_from_utf8(buffer: Vec<u8>) -> Result<String,String> {
    match String::from_utf8(buffer) {
        Ok(buffer) => {
            Ok(buffer)
        },
        Err(_) => {
            return Err("Non utf8 characters in command".to_string());
        }
    }
}


fn parse_command_name(buffer: &mut Chars) -> Result<String, String> {
    let mut command_name = String::new();
    for c in buffer {
        match c {
            '\'' => {
                return Err("Malformed command".to_string());
            },
            '\\' => {
                return Err("Malformed command".to_string());
            },
            ' ' => {
                break;
            },
            _ => {
                command_name.push(c);
            }
        }
    }
    Ok(command_name.to_uppercase())
}

fn parse_arguments(buffer: &mut Chars) -> Result<Vec<String>,String>{
    let mut arguments = Vec::new();
    loop {
        match try!(parse_argument(buffer)) {
            Some(argument) => {
                arguments.push(argument);
            }
            None => {
                break;
            }
        }
    }
    Ok(arguments)
}

fn parse_argument(buffer: &mut Chars) -> Result<Option<String>, String> {
    loop {
        match buffer.skip_while(|c| *c == ' ').next() {
            Some(c) => {
                match c {
                    '\'' => {
                        let result = try!(parse_quoted_string(buffer));
                        return Ok(Some(result));
                    },
                    _ => {
                        return Err(format!("Unqouted character: {}", c));
                    },
                }
            },
            None => {
                return Ok(None);
            }
        }
    }
}

fn parse_quoted_string(buffer: &mut Chars) -> Result<String, String> {
    let mut current_string = String::new();
    loop {
        match buffer.next() {
            Some(c) => {
                match c {
                    '\'' => {
                        return Ok(current_string);
                    },
                    '\\' => {
                        let c = try!(parse_escaped_string(buffer));
                        current_string.push(c);
                    },
                    c => {
                        current_string.push(c);
                    }
                }
            },
            None => {
                return Err("Missing end quote".to_string());
            }
        }
    }
}

fn parse_escaped_string(buffer: &mut Chars) -> Result<char, String> {
    let result = buffer.next();
    match result {
        Some(c) => {
            match c {
                '\'' => {
                    return Ok('\'');
                },
                '\\' => {
                    return Ok('\\');
                },
                c => {
                    return Err(format!("Unescapeable character: {}", c));
                }
            }
        },
        None => {
            return Err("Backslash must be followed by \\ or '".to_string());
        }
    }
}

fn build_command(command_name: String, arguments: Vec<String>) -> ParseResult {
    match &command_name as &str {
        "PUSH"   => { build_push(arguments) }
        "POP"    => { build_pop(arguments) }
        "BPOP"   => { build_bpop(arguments) }
        "QUIT"   => { build_with_no_args(arguments, "QUIT", Command::Quit) },
        "BEGIN"  => { build_with_no_args(arguments, "BEGIN", Command::Begin) },
        "COMMIT" => { build_with_no_args(arguments, "COMMIT", Command::Commit) },
        "ABORT"  => { build_with_no_args(arguments, "ABORT", Command::Abort) },
        cmd      => Err(format!("Unknown Command: {}", cmd))
    }
}

fn build_push(arguments: Vec<String>) -> Result<Command, String> {
    if arguments.len() == 2 {
        let queue_name = arguments[0].clone();
        let value = arguments[1].clone();
        Ok(Command::Push(value, queue_name))
    } else {
        Err("Incorrect number of arguments for PUSH".to_string())
    }
}

fn build_pop(arguments: Vec<String>) -> Result<Command, String> {
    if arguments.len() == 1 {
        let queue_name = arguments[0].clone();
        Ok(Command::Pop(queue_name))
    } else {
        Err("Incorrect number of arguments for POP".to_string())
    }
}

fn build_bpop(arguments: Vec<String>) -> Result<Command, String> {
    if arguments.len() == 1 {
        let queue_name = arguments[0].clone();
        Ok(Command::BlockingPop(queue_name))
    } else {
        Err("Incorrect number of arguments for BPOP".to_string())
    }
}

fn build_with_no_args(arguments: Vec<String>, command_name: &'static str, command: Command) -> Result<Command, String> {
    if arguments.len() == 0 {
        Ok(command)
    } else {
        Err(format!("No arguments expect for command: {}", command_name))
    }
}
