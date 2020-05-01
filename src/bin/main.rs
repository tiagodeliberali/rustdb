use rustdb::{KeyValue, RustDB};
use serde_json::Value;
use std::io::prelude::*;
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

const INSERT_DATA: &[u8; 17] = b"POST / HTTP/1.1\r\n";
const UPDATE_DATA: &[u8; 16] = b"PUT / HTTP/1.1\r\n";
const DELETE_DATA: &[u8; 19] = b"DELETE / HTTP/1.1\r\n";
const READ_DATA: &[u8; 16] = b"GET / HTTP/1.1\r\n";

fn main() {
    println!("Loading database...");
    let mut db = RustDB::open();
    db.load();
    let mut db = Arc::new(Mutex::new(db));
    let listener = TcpListener::bind("127.0.0.1:7887").unwrap();
    println!("Database ready at 7887");

    for stream in listener.incoming() {
        let stream = stream.unwrap();
        let db_clone = Arc::clone(&db);
        handle_connection(stream, db_clone);
    }
}

fn handle_connection(mut stream: TcpStream, db: Arc<Mutex<RustDB>>) {
    let mut buffer = [0; 512];
    let size = stream.read(&mut buffer).unwrap();

    let request = String::from_utf8_lossy(&buffer[..size]);
    let content: Vec<&str> = request.split("\r\n\r\n").collect();
    let content = content[1];

    let key_value = match extract_keyvalue(content) {
        Ok(v) => v,
        Err(err) => {
            println!("[ERROR] Keyvalue parse error: {}", err);
            return_http(
                stream,
                &format!(
                    "HTTP/1.1 400 BAD REQUEST\r\n\r\nInvalid json payload\n{}",
                    err
                ),
            );
            return;
        }
    };

    let (response_code, action) = if buffer.starts_with(READ_DATA) {
        let key = key_value.get_key_as_string();

        let (response_code, action) = match db.lock().unwrap().get_record(key.to_string()) {
            Ok(key_value) => ("200 OK", format!("action :READ\nid: {}\nvalue: {}", key_value.get_key_as_string(), key_value.get_value_as_string().unwrap())),
            Err(err) => ("500 INTERNAL SERVER ERROR", format!("action :READ\nid: {}\nerror: {}", key, err)),
        };
        (response_code, action)
    } else if buffer.starts_with(INSERT_DATA) {
        match key_value.get_value_as_string() {
            Some(value) => {
                let key = key_value.get_key_as_string();

                let (response_code, action) = match db.lock().unwrap().save_record(key_value) {
                    Ok(_) => ("200 OK", format!("action :INSERT\nid: {}\nvalue: {}", key, value)),
                    Err(err) => ("500 INTERNAL SERVER ERROR", format!("action :INSERT\nid: {}\nvalue: {}\nerror: {}", key, value, err)),
                };

                (response_code, action)
            },
            None => ("400 BAD REQUEST", String::from("Missing value to INSERT")),
        }
    } else if buffer.starts_with(UPDATE_DATA) {
        match key_value.get_value_as_string() {
            Some(value) => (
                "200 OK",
                format!(
                    "action :UPDATE\nid: {}\nvalue: {}",
                    key_value.get_key_as_string(),
                    value
                ),
            ),
            None => ("400 BAD REQUEST", String::from("Missing value to UPDATE")),
        }
    } else if buffer.starts_with(DELETE_DATA) {
        (
            "200 OK",
            format!("action :DELETE\nid: {}", key_value.get_key_as_string()),
        )
    } else {
        ("400 BAD REQUEST", String::from("unknown comand"))
    };

    let response = format!("HTTP/1.1 {}\r\n\r\n{}", response_code, action);
    return_http(stream, &response);
}

fn extract_keyvalue(content: &str) -> Result<KeyValue, String> {
    let v: Value = match serde_json::from_str(content) {
        Ok(value) => value,
        Err(error) => return Err(format!("{}", error)),
    };

    let (key, value) = match v {
        Value::Null => return Err(String::from("Invalid input: null")),
        Value::Bool(_) => return Err(String::from("Invalid input: boolean")),
        Value::Object(obj) if !obj.contains_key("id") => {
            return Err(String::from("Missing field 'id'"))
        }
        Value::Array(_) => return Err(String::from("Invalid input: array")),
        Value::Number(id) => (id.to_string(), None),
        Value::String(id) => (id, None),
        Value::Object(obj) => {
            let id = obj["id"].to_string().replace("\"", "");
            if obj.keys().len() == 1 {
                (id, None)
            } else {
                (id, Some(Value::Object(obj).to_string()))
            }
        }
    };

    match value {
        Some(v) => Ok(KeyValue::new_from_strings(key, v)),
        None => Ok(KeyValue::new_no_value(key)),
    }
}

fn return_http(mut stream: TcpStream, response: &str) {
    let _ = stream.write(response.as_bytes()).unwrap();
    stream.flush().unwrap();
}
