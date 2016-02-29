use std::thread;
//use std::sync::mpsc;
use std::net::{TcpListener, TcpStream};
use std::io::{Write, Read};

fn handle_stream(mut stream: TcpStream) {
    let _ = stream.write(b"hello world");
    let _ = stream.flush();
    let mut buffer = [0; 10];
    let _ = stream.read(&mut buffer);
    let _ = stream.write(&mut buffer);
}

fn main() {
    //let (tx, rx) = mpsc::channel();
    let listener = TcpListener::bind("127.0.0.1:5248").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                thread::spawn(move|| {
                    handle_stream(stream);
                });
            }
            Err(_) => {
            }
        }
    }

    //for i in 0..10 {
        //let tx = tx.clone();

        //thread::spawn(move || {
            //let answer = i * i;

            //tx.send(answer).unwrap();
        //});
    //}

    //for _ in 0..10 {
        //println!("{}", rx.recv().unwrap());
    //}
}
