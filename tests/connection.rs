#![feature(plugin)]
#![cfg_attr(test, plugin(stainless))]

#[cfg(test)]
mod tests {
    extern crate queue_experiments;
    pub use self::queue_experiments::connection::{Connection};
    pub use self::queue_experiments::queue_table::{QueueTable};
    pub use std::io::Cursor;
    describe! connection {

        it "listen_stops_for_quit_command" {
            let queue_table = QueueTable::new();
            let mut writer = Cursor::new(Vec::new());
            let mut reader = Cursor::new(b"QUIT");

            let mut connection = Connection::new(&mut reader, &mut writer, &queue_table);

            connection.listen();
        }

        it "process_message_executes_push" {
            let queue_table = QueueTable::new();
            let mut writer = Cursor::new(Vec::new());
            let mut reader = Cursor::new(b"PUSH 'queue' 'data';");

            {
                let mut connection = Connection::new(&mut reader, &mut writer, &queue_table);
                connection.process_message();
            }

            let queue = queue_table.get_queue(&"queue".to_string()).unwrap();
            assert_eq!(queue.pop_front(), Some("data".to_string()));
        }

        it "process_message_outputs_success_for_push" {
            let queue_table = QueueTable::new();
            let mut writer = Cursor::new(Vec::new());
            let mut reader = Cursor::new(b"PUSH 'queue' 'data';");

            {
                let mut connection = Connection::new(&mut reader, &mut writer, &queue_table);
                connection.process_message();
            }
            assert_eq!(String::from_utf8(writer.into_inner()).unwrap(), "SUCCESS\r\n".to_string());
        }

        it "process_message_executes_pop" {
            let queue_table = QueueTable::new();
            let mut writer = Cursor::new(Vec::new());
            let mut reader = Cursor::new(b"POP 'queue';");

            let queue = queue_table.get_or_create_queue("queue".to_string());
            queue.push_back("data".to_string());

            {
                let mut connection = Connection::new(&mut reader, &mut writer, &queue_table);
                connection.process_message();
            }

            let queue = queue_table.get_queue(&"queue".to_string()).unwrap();
            assert_eq!(queue.pop_front(), None);
        }

        it "process_message_outputs_value_for_pop" {
            let queue_table = QueueTable::new();
            let mut writer = Cursor::new(Vec::new());
            let mut reader = Cursor::new(b"POP 'queue';");

            let queue = queue_table.get_or_create_queue("queue".to_string());
            queue.push_back("data".to_string());

            {
                let mut connection = Connection::new(&mut reader, &mut writer, &queue_table);
                connection.process_message();
            }
            assert_eq!(String::from_utf8(writer.into_inner()).unwrap(), "data\r\n".to_string());
        }
    }
}
