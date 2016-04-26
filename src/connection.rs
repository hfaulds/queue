use std::io::{Write, BufRead};

use queue_table::{QueueTable};
use commands::{Command};
use transaction::{Transaction,TransactionResult};

pub struct Connection<'a> {
    queue_table: &'a QueueTable,
    reader: &'a mut BufRead,
    writer: &'a mut Write,
    transaction: Transaction,
}

impl <'a>Connection<'a> {
    pub fn new(reader: &'a mut BufRead, writer: &'a mut Write, queue_table: &'a QueueTable) -> Connection<'a> {
        Connection {
            queue_table: &queue_table,
            reader: reader,
            writer: writer,
            transaction: Transaction::new(),
        }
    }

    pub fn listen(&mut self) {
        loop {
            let result = self.process_message();
            self.flush();
            if result.is_quit() {
                self.transaction.rollback(&self.queue_table);
                break;
            }
        }
    }

    pub fn process_message(&mut self) -> TransactionResult {
        match self.read_message() {
            Ok(result) => {
                let cmd = Command::parse(result);
                match cmd {
                    Ok(cmd) => {
                        self.handle_command(cmd)
                    }
                    Err(message) => {
                        self.write(format!("{}\r\n", message).as_bytes());
                        TransactionResult::Continue
                    }
                }
            }
            Err(_) => {
                TransactionResult::Quit
            }
        }
    }

    pub fn handle_command(&mut self, cmd: Command) -> TransactionResult {
        let transaction_result = self.transaction.exec(cmd, &self.queue_table);
        let command_result = transaction_result.command_result();
        transaction_result.write(self.writer);
        command_result
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
}
