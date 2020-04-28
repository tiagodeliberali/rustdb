pub struct KeyValue {
    pub key: String,
    pub value: Option<String>,
}

impl KeyValue {
    pub fn new(key: String, value: String) -> KeyValue {
        KeyValue {
            key,
            value: Option::Some(value),
        }
    }

    pub fn new_no_value(key: String) -> KeyValue {
        KeyValue {
            key,
            value: Option::None,
        }
    }
}
