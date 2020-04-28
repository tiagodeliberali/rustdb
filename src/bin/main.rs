use rustdb::KeyValue;
use std::io::prelude::*;
use std::net::{TcpListener, TcpStream};
use serde_json::{Value};

const INSERT_DATA: &[u8; 17] = b"POST / HTTP/1.1\r\n";
const UPDATE_DATA: &[u8; 16] = b"PUT / HTTP/1.1\r\n";
const DELETE_DATA: &[u8; 19] = b"DELETE / HTTP/1.1\r\n";
const READ_DATA: &[u8; 16] = b"GET / HTTP/1.1\r\n";

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
    let size = stream.read(&mut buffer).unwrap();

    let request = String::from_utf8_lossy(&buffer[..size]);
    let content: Vec<&str> = request.split("\r\n\r\n").collect();
    let content = content[1];

    let key_value = match extract_keyvalue(content) {
        Ok(v) => v,
        Err(err) => {
            println!("[ERROR] Keyvalue parse error: {}", err);
            return_http(stream, "HTTP/1.1 400 BAD REQUEST\r\n\r\nInvalid json payload");
            return;
        },
    };

    let (response_code, action) = if buffer.starts_with(READ_DATA) {
        ("200 OK", format!("action :READ\nid: {}", key_value.key))
    } else if buffer.starts_with(INSERT_DATA) {
        match key_value.value {
            Some(value) => ("200 OK", format!("action :INSERT\nid: {}\nvalue: {}", key_value.key, value)),
            None => ("400 BAD REQUEST", String::from("Missing value to INSERT")),
        }
    } else if buffer.starts_with(UPDATE_DATA) {
        match key_value.value {
            Some(value) => ("200 OK", format!("action :UPDATE\nid: {}\nvalue: {}", key_value.key, value)),
            None => ("400 BAD REQUEST", String::from("Missing value to UPDATE")),
        }
    } else if buffer.starts_with(DELETE_DATA) {
        ("200 OK", format!("action :DELETE\nid: {}", key_value.key))
    } else {
        ("200 OK", String::from("unknown comand"))
    };

    let response = format!("HTTP/1.1 {}\r\n\r\n{}", response_code, action);
    return_http(stream, &response);
}

fn extract_keyvalue(content: &str) -> Result<KeyValue, String> {
    let v: Value = match serde_json::from_str(content) {
        Ok(value) => value,
        Err(error) => return Err(format!("{}", error)),
    };

    if v["id"] == Value::Null {
        return Err(String::from("Missing field 'id'"));
    }

    return Ok(KeyValue::new(v["id"].to_string(), v.to_string()));
}

fn return_http(mut stream: TcpStream, response: &str) {
    let _ = stream.write(response.as_bytes()).unwrap();
    stream.flush().unwrap();
}
