pub type ByteString = Vec<u8>;

pub struct KeyValue {
    pub key: ByteString,
    pub value: ByteString,
}

impl KeyValue {
    pub fn new_from_strings(key: String, value: String) -> KeyValue {
        KeyValue {
            key: key.into_bytes(),
            value: value.into_bytes(),
        }
    }

    pub fn new(key: ByteString, value: ByteString) -> KeyValue {
        KeyValue { key, value }
    }

    pub fn get_key_as_string(&self) -> String {
        String::from_utf8_lossy(&self.key).into_owned()
    }

    pub fn get_value_as_string(&self) -> String {
        String::from_utf8_lossy(&self.value).into_owned()
    }
}
