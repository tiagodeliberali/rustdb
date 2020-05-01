use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc::crc32;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{
    prelude::*, BufReader, Error, ErrorKind, ErrorKind::UnexpectedEof, Result, SeekFrom,
};
use std::path::Path;

type ByteString = Vec<u8>;
// type ByteStr = [u8];

pub struct KeyValue {
    key: ByteString,
    value: ByteString,
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

pub struct RustDB {
    database_file: File,
    index: HashMap<ByteString, u64>,
}

impl RustDB {
    pub fn open() -> RustDB {
        let database_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(Path::new("./current_db"))
            .unwrap();

        RustDB {
            database_file,
            index: HashMap::new(),
        }
    }

    pub fn load(&mut self) -> Result<()> {
        let mut database_buffer = BufReader::new(&self.database_file);

        loop {
            let current_position = database_buffer.seek(SeekFrom::Current(0))?;

            match RustDB::load_record(&mut database_buffer) {
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

        let calculated_checksum = crc32::checksum_ieee(&data);

        if checksum != calculated_checksum {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!(
                    "Invalid checksum at position: {}\nExpected: {}\nFound: {}",
                    file.seek(SeekFrom::Current(0))?,
                    calculated_checksum,
                    checksum
                ),
            ));
        }

        let (key, value) = data.split_at(key_size);

        Ok(KeyValue::new(key.to_vec(), value.to_vec()))
    }

    pub fn get_record(&self, key: String) -> Result<KeyValue> {
        let key: Vec<u8> = Vec::from(key);
        let key_position = match self.index.get(&key) {
            Some(position) => position,
            None => return Err(Error::from(ErrorKind::NotFound)),
        };

        let mut buffer = BufReader::new(&self.database_file);
        let _ = buffer.seek(SeekFrom::Start(*key_position))?;

        match RustDB::load_record(&mut buffer) {
            Ok(data) => Ok(data),
            Err(err) => Err(err),
        }
    }

    pub fn delete_record(&self, key: String) -> Result<()> {
        let key: Vec<u8> = Vec::from(key);
        Ok(())
    }

    pub fn save_record(&mut self, mut key_value: KeyValue) -> Result<usize> {
        let mut value = key_value.value;
        let key_size = key_value.key.len() as u32;
        let value_size = value.len() as u32;
        let total_size = key_size + value_size;
        let mut data: Vec<u8> = Vec::with_capacity(total_size as usize);

        data.append(&mut key_value.key);
        data.append(&mut value);
        let checksum = crc32::checksum_ieee(&data);

        self.database_file.write_u32::<LittleEndian>(checksum)?;
        self.database_file.write_u32::<LittleEndian>(key_size)?;
        self.database_file.write_u32::<LittleEndian>(value_size)?;
        self.database_file.write(&data)?;

        Ok(self.database_file.seek(SeekFrom::Current(0))? as usize)
    }
}
