use rustdb::{KeyValue, RustDB};
use serde_json::Value;
use std::collections::HashMap;
use std::io::prelude::*;
use std::net::{TcpListener, TcpStream};

const INSERT_DATA: &[u8; 17] = b"POST / HTTP/1.1\r\n";
const UPDATE_DATA: &[u8; 16] = b"PUT / HTTP/1.1\r\n";
const DELETE_DATA: &[u8; 19] = b"DELETE / HTTP/1.1\r\n";
const READ_DATA: &[u8; 16] = b"GET / HTTP/1.1\r\n";

#[cfg(debug_assertions)]
fn debug(msg: &str) {
    println!("[DEBUG INFO]: {}", msg);
}

#[cfg(not(debug_assertions))]
fn debug(_msg: &str) {
    // do nothing
}

fn main() {
    println!("Loading database...");
    let mut db = RustDB::new("storage");
    let listener = match TcpListener::bind("127.0.0.1:7887") {
        Ok(listener) => listener,
        Err(err) => panic!("Failed to bind address\n{}", err),
    };
    println!("Database ready at 7887");

    // for now: single threaded - no concurrency
    for stream in listener.incoming() {
        match stream {
            Ok(result) => handle_connection(result, &mut db),
            Err(err) => println!("Failed to process current stream\n{}", err),
        };
    }
}

fn handle_connection(mut stream: TcpStream, db: &mut RustDB) {
    let mut buffer = [0; 512];
    let size = match stream.read(&mut buffer) {
        Ok(value) => value,
        Err(err) => {
            println!("Failed to read stream\n{}", err);
            return;
        }
    };

    let content = String::from_utf8_lossy(&buffer[..size]);
    let content: Vec<&str> = content.split("\r\n\r\n").collect();
    let content = content[1];

    let mut response = Response::new(400, String::new());

    for (action_type, action) in build_actions().into_iter() {
        if buffer.starts_with(action_type) {
            response = action(content, db);
        }
    }

    debug(&format!(
        "status_code: {} - response: {}",
        response.status_code, response.response
    ));

    match stream.write(build_response(response).as_bytes()) {
        Ok(_) => {
            if let Err(err) = stream.flush() {
                println!("Failed to flush stream\n{}", err);
            }
        }
        Err(err) => println!("Failed to write to stream\n{}", err),
    };
}

fn build_response(response: Response) -> String {
    let status_code = match response.status_code {
        200 => "200 OK",
        204 => "204 NO CONTENT",
        400 => "400 BAD REQUEST",
        _ => "500 INTERNAL SERVER ERROR",
    };
    format!("HTTP/1.1 {}\r\n\r\n{}", status_code, response.response)
}

struct Response {
    status_code: u16,
    response: String,
}

impl Response {
    fn new(status_code: u16, response: String) -> Response {
        Response {
            status_code,
            response,
        }
    }
}

type Callback = fn(&str, &mut RustDB) -> Response;

fn build_actions() -> HashMap<&'static [u8], Callback> {
    let mut actions: HashMap<&[u8], Callback> = HashMap::new();
    actions.insert(READ_DATA, read_content);
    actions.insert(DELETE_DATA, delete_content);
    actions.insert(INSERT_DATA, update_content);
    actions.insert(UPDATE_DATA, update_content);

    actions
}

fn read_content(content: &str, db: &mut RustDB) -> Response {
    let key = match get_key(content) {
        Ok(v) => v,
        Err(err) => return Response::new(400, err),
    };

    let (response_code, result) = match db.get_record(key) {
        Ok(key_value) => match key_value {
            Some(kv) => (200, kv.get_value_as_string()),
            None => (204, String::new()),
        },
        Err(err) => (500, err.to_string()),
    };

    Response::new(response_code, result)
}

fn delete_content(content: &str, db: &mut RustDB) -> Response {
    let key = match get_key(content) {
        Ok(v) => v,
        Err(err) => return Response::new(400, err),
    };

    let (response_code, result) = match db.delete_record(key) {
        Ok(_) => (200, String::new()),
        Err(err) => (500, err.to_string()),
    };

    Response::new(response_code, result)
}

fn update_content(content: &str, db: &mut RustDB) -> Response {
    let key_value = match get_keyvalue(content) {
        Ok(v) => v,
        Err(err) => return Response::new(400, err),
    };

    let (response_code, result) = match db.save_record(key_value) {
        Ok(_) => (200, String::new()),
        Err(err) => (500, err.to_string()),
    };

    Response::new(response_code, result)
}

fn get_key(content: &str) -> Result<String, String> {
    let json_content: Value = match serde_json::from_str(content) {
        Ok(value) => value,
        Err(error) => return Err(error.to_string()),
    };

    let key = match json_content {
        Value::Null => return Err(String::from("Invalid input: null")),
        Value::Bool(_) => return Err(String::from("Invalid input: boolean")),
        Value::Object(obj) if !obj.contains_key("id") => {
            return Err(String::from("Missing field 'id'"))
        }
        Value::Array(_) => return Err(String::from("Invalid input: array")),
        Value::Number(id) => id.to_string(),
        Value::String(id) => id,
        Value::Object(obj) => obj["id"].to_string().replace("\"", ""),
    };

    Ok(key)
}

fn get_keyvalue(content: &str) -> Result<KeyValue, String> {
    let key = get_key(content)?;

    let v: Value = match serde_json::from_str(content) {
        Ok(value) => value,
        Err(error) => return Err(error.to_string()),
    };

    let value = match v {
        Value::Object(obj) => {
            if obj.keys().len() > 1 {
                Value::Object(obj).to_string()
            } else {
                return Err(String::from(
                    "Invalid input: Missing fields different from id",
                ));
            }
        }
        _ => return Err(String::from("Invalid input: Missing value")),
    };

    Ok(KeyValue::new_from_strings(key, value))
}
