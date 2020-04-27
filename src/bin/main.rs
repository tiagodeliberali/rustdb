use std::io::prelude::*;
use std::net::{TcpListener, TcpStream};

fn main() {
    println!("Load database at 7887");
    let listener = TcpListener::bind("127.0.0.1:7887").unwrap();

    for stream in listener.incoming() {
        let stream = stream.unwrap();
        handle_connection(stream);
    }
}

fn handle_connection(mut stream: TcpStream) {
    let mut buffer = [0; 512];
    let _ = stream.read(&mut buffer).unwrap();

    let insert_data = b"POST / HTTP/1.1\r\n";
    let read_data = b"GET / HTTP/1.1\r\n";

    let action = if buffer.starts_with(read_data) {
        "read data"
    } else if buffer.starts_with(insert_data) {
        "insert data"
    } else {
        "unknown comand"
    };

    let content = String::from_utf8_lossy(&buffer[..]);
    let start_content: Vec<&str> = content.split("\r\n\r\n").collect();
    let key_value: Vec<&str> = start_content[1].split('|').collect();
    let key = key_value[0];
    let value = key_value[1];

    println!("id: {}\r\ncontent: {}", key, value);

    let response = format!("{}\r\n\r\n{}", "HTTP/1.1 200 OK", action);

    let _ = stream.write(response.as_bytes()).unwrap();
    stream.flush().unwrap();
}
