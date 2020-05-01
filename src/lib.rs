use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc::crc32;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{prelude::*, BufReader, ErrorKind::UnexpectedEof, Result, SeekFrom};
use std::path::Path;

type ByteString = Vec<u8>;
type ByteStr = [u8];

pub struct KeyValue {
    key: ByteString,
    value: Option<ByteString>,
}

impl KeyValue {
    pub fn new_from_strings(key: String, value: String) -> KeyValue {
        KeyValue {
            key: key.into_bytes(),
            value: Option::Some(value.into_bytes()),
        }
    }

    pub fn new(key: ByteString, value: ByteString) -> KeyValue {
        KeyValue {
            key: key,
            value: Option::Some(value),
        }
    }

    pub fn new_no_value(key: String) -> KeyValue {
        KeyValue {
            key: key.into_bytes(),
            value: Option::None,
        }
    }

    pub fn get_key_as_string(&self) -> String {
        String::from_utf8_lossy(&self.key).into_owned()
    }

    pub fn get_value_as_string(&self) -> Option<String> {
        match &self.value {
            Some(value) => Some(String::from_utf8_lossy(&value).into_owned()),
            None => None,
        }
    }
}

pub struct RustDB {
    f: File,
    index: HashMap<ByteString, u64>,
}

impl RustDB {
    pub fn open() -> RustDB {
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(Path::new("./current_db"))
            .unwrap();

        RustDB {
            f,
            index: HashMap::new(),
        }
    }

    pub fn load(&mut self) -> Result<()> {
        let mut f = BufReader::new(&self.f);

        loop {
            let current_position = f.seek(SeekFrom::Current(0))?;
            match RustDB::load_record(&mut f) {
                Ok(key_value) => self.index.insert(key_value.key, current_position),
                Err(err) => match err.kind() {
                    UnexpectedEof => {
                        break;
                    }
                    _ => return Err(err),
                },
            };
        }

        Ok(())
    }

    fn load_record(file: &mut BufReader<&File>) -> Result<KeyValue> {
        let checksum = file.read_u32::<LittleEndian>()?;
        let key_size: usize = file.read_u32::<LittleEndian>()? as usize;
        let value_size: usize = file.read_u32::<LittleEndian>()? as usize;
        let total_size: usize = key_size + value_size;

        let mut data = ByteString::with_capacity(total_size);

        {
            file.by_ref()
                .take(total_size as u64)
                .read_to_end(&mut data)?;
        }

        if checksum != crc32::checksum_ieee(&data) {
            panic!("invalid checksum");
        }

        let (key, value) = data.split_at(key_size);

        Ok(KeyValue::new(key.to_vec(), value.to_vec()))
    }

    pub fn get_record(&self, key: String) -> Result<KeyValue> {
        let key: Vec<u8> = Vec::from(key);
        let key_position = *self.index.get(&key).unwrap();

        let mut f = BufReader::new(&self.f);
        let _ = f.seek(SeekFrom::Start(key_position))?;

        Ok(RustDB::load_record(&mut f).unwrap())
    }

    pub fn save_record(&mut self, mut key_value: KeyValue) -> Result<usize> {
        let mut value = key_value.value.unwrap();
        let key_size = key_value.key.len() as u32;
        let value_size = value.len() as u32;
        let total_size = key_size + value_size;
        let mut data: Vec<u8> = Vec::with_capacity(total_size as usize);

        data.append(&mut key_value.key);
        data.append(&mut value);
        let checksum = crc32::checksum_ieee(&data);

        self.f.write_u32::<LittleEndian>(checksum)?;
        self.f.write_u32::<LittleEndian>(key_size)?;
        self.f.write_u32::<LittleEndian>(value_size)?;
        self.f.write(&data)?;

        Ok(self.f.seek(SeekFrom::Current(0))? as usize)
    }
}
