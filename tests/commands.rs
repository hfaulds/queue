#![feature(plugin)]
#![cfg_attr(test, plugin(stainless))]

#[cfg(test)]
mod tests {
    extern crate queue_experiments;
    pub use self::queue_experiments::commands::{Command};

    describe! commands {
        it "it_parses_push_commands" {
            assert_eq!(
                Command::parse("PUSH 'a' 'b'".to_string().into_bytes()),
                Ok(Command::Push("b".to_string(), "a".to_string()))
                );
        }

        it "it_parses_pop_commands" {
            assert_eq!(
                Command::parse("POP 'a'".to_string().into_bytes()),
                Ok(Command::Pop("a".to_string()))
                );
        }

        it "it_parses_bpop_commands" {
            assert_eq!(
                Command::parse("BPOP 'a'".to_string().into_bytes()),
                Ok(Command::BlockingPop("a".to_string()))
                )
        }

        it "it_parses_quit_commands" {
            assert_eq!(
                Command::parse("QUIT".to_string().into_bytes()),
                Ok(Command::Quit)
                );
        }

        it "it_parses_begin_commands" {
            assert_eq!(
                Command::parse("BEGIN".to_string().into_bytes()),
                Ok(Command::Begin)
                );
        }

        it "it_parses_abort_commands" {
            assert_eq!(
                Command::parse("ABORT".to_string().into_bytes()),
                Ok(Command::Abort)
                );
        }

        it "it_returns_err_for_unknown_commands" {
            assert_eq!(
                Command::parse("FoO".to_string().into_bytes()),
                Err("Unknown Command: FOO".to_string())
                );
        }

        it "it_parses_commands_case_insensitively" {
            assert_eq!(
                Command::parse("BeGiN".to_string().into_bytes()),
                Ok(Command::Begin)
                );
        }

        it "it_returns_malformed_argument_err_if_quote_in_command" {
            assert_eq!(
                Command::parse("A'B".to_string().into_bytes()),
                Err("Malformed command".to_string())
                );
        }

        it "it_returns_malformed_argument_err_if_slash_in_command" {
            assert_eq!(
                Command::parse("A\\B".to_string().into_bytes()),
                Err("Malformed command".to_string())
                );
        }

        it "it_allows_escaped_quotes_in_arguments" {
            assert_eq!(
                Command::parse("POP 'a\\''".to_string().into_bytes()),
                Ok(Command::Pop("a'".to_string()))
                );
        }

        it "it_allows_escaped_backslashes_in_arguments" {
            assert_eq!(
                Command::parse("POP 'a\\\\'".to_string().into_bytes()),
                Ok(Command::Pop("a\\".to_string()))
                );
        }

        it "it_returns_err_for_unquoted_characters_in_argument" {
            assert_eq!(
                Command::parse("POP a".to_string().into_bytes()),
                Err("unqouted character: a".to_string())
                );
        }

        it "it_returns_err_for_unescaped_characters_in_argument" {
            assert_eq!(
                Command::parse("POP '\\a'".to_string().into_bytes()),
                Err("unescapeable character: a".to_string())
                );
        }

        it "it_returns_err_for_missing_end_quote_in_argument" {
            assert_eq!(
                Command::parse("POP 'a' 'b".to_string().into_bytes()),
                Err("missing end quote".to_string())
                );
        }

        it "it_returns_err_for_wrong_number_of_arguments" {
            assert_eq!(
                Command::parse("PUSH 'a' 'b' 'c'".to_string().into_bytes()),
                Err("Incorrect number of arguments for PUSH".to_string())
                );
        }
    }
}
