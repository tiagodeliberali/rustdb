use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc::crc32;
use rand::random;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{
    prelude::*, BufReader, Error, ErrorKind, ErrorKind::UnexpectedEof, Result, SeekFrom,
};
use std::path::Path;

type ByteString = Vec<u8>;

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

struct DataSgment {
    database_file: File,
    index: HashMap<ByteString, u64>,
    closed: bool,
    previous: Option<Box<DataSgment>>,
    size: u64,
}

impl DataSgment {
    fn new(folder: &str) -> DataSgment {
        let database_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(Path::new(&format!(
                "./{}/current_db_{}",
                folder,
                random::<u64>()
            )))
            .unwrap();

        let mut buffer = BufReader::new(&database_file);
        let size = buffer.seek(SeekFrom::End(0)).unwrap();

        DataSgment {
            database_file,
            index: HashMap::new(),
            closed: false,
            previous: None,
            size,
        }
    }

    fn open(file_name: &str) -> DataSgment {
        let database_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(Path::new(file_name))
            .unwrap();

        let mut buffer = BufReader::new(&database_file);
        let size = buffer.seek(SeekFrom::End(0)).unwrap();

        let mut segment = DataSgment {
            database_file,
            index: HashMap::new(),
            closed: true,
            previous: None,
            size,
        };

        segment.load().unwrap();

        segment
    }

    fn load(&mut self) -> Result<()> {
        let mut database_buffer = BufReader::new(&self.database_file);
        let _ = database_buffer.seek(SeekFrom::Start(0))?;

        loop {
            let current_position = database_buffer.seek(SeekFrom::Current(0))?;

            match DataSgment::load_record(&mut database_buffer) {
                Ok(key_value) => {
                    DataSgment::update_index(&mut self.index, &key_value, current_position)
                }
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

    fn update_index(index: &mut HashMap<ByteString, u64>, key_value: &KeyValue, position: u64) {
        if key_value.value.len() == 0 && index.contains_key(&key_value.key) {
            index.remove(&key_value.key);
        } else {
            index.insert(key_value.key.to_owned(), position);
        }
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

    fn get_record(&self, key: String) -> Result<Option<KeyValue>> {
        let key: Vec<u8> = Vec::from(key);
        let key_position = match self.index.get(&key) {
            Some(position) => position,
            None => return Ok(None),
        };

        let mut buffer = BufReader::new(&self.database_file);
        let _ = buffer.seek(SeekFrom::Start(*key_position))?;

        match DataSgment::load_record(&mut buffer) {
            Ok(data) => Ok(Some(data)),
            Err(err) => Err(err),
        }
    }

    fn delete_record(&mut self, key: String) -> Result<()> {
        let key: Vec<u8> = Vec::from(key);
        self.save_record(KeyValue::new(key, Vec::new()))?;
        Ok(())
    }

    fn save_record(&mut self, key_value: KeyValue) -> Result<()> {
        let position = self.database_file.seek(SeekFrom::End(0))?;

        let key_size = key_value.key.len() as u32;
        let value_size = key_value.value.len() as u32;
        let total_size = key_size + value_size;
        let mut data: Vec<u8> = Vec::with_capacity(total_size as usize);

        data.append(&mut key_value.key.clone());
        data.append(&mut key_value.value.clone());
        let checksum = crc32::checksum_ieee(&data);

        self.database_file.write_u32::<LittleEndian>(checksum)?;
        self.database_file.write_u32::<LittleEndian>(key_size)?;
        self.database_file.write_u32::<LittleEndian>(value_size)?;
        let _ = self.database_file.write(&data)?;

        DataSgment::update_index(&mut self.index, &key_value, position);

        Ok(())
    }
}

pub struct RustDB {
    segment: DataSgment,
}

impl RustDB {
    pub fn open(file_name: &str) -> RustDB {
        RustDB {
            segment: DataSgment::open(file_name),
        }
    }

    pub fn new(folder: &str) -> RustDB {
        RustDB {
            segment: DataSgment::new(folder),
        }
    }

    pub fn get_record(&self, key: String) -> Result<Option<KeyValue>> {
        self.segment.get_record(key)
    }

    pub fn delete_record(&mut self, key: String) -> Result<()> {
        self.segment.delete_record(key)
    }

    pub fn save_record(&mut self, key_value: KeyValue) -> Result<()> {
        self.segment.save_record(key_value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    static STORAGE_TEST_FOLDER: &str = "storage_test";
    static STORAGE_TEST_FILE: &str = "./storage_test/current_db";

    #[test]
    fn create_empty_segment_on_new_db() {
        let db = RustDB::new(STORAGE_TEST_FOLDER);

        let segment = db.segment;

        assert_eq!(segment.closed, false);
        assert_eq!(segment.size, 0);
        assert_eq!(segment.previous.is_none(), true);
    }

    #[test]
    fn open_existing_segment() {
        let db = RustDB::open(STORAGE_TEST_FILE);

        let segment = db.segment;

        assert_eq!(segment.closed, true);
        assert_eq!(segment.size, 69);
        assert_eq!(segment.previous.is_none(), true);
    }

    #[test]
    fn open_existing_segment_and_find_record() {
        let db = RustDB::open(STORAGE_TEST_FILE);

        let data = db.get_record(String::from("1234"));
        assert_eq!(data.is_ok(), true);

        let data = data.unwrap();
        assert_eq!(data.is_some(), true);

        let data = data.unwrap();
        assert_eq!(data.get_key_as_string(), "1234");
        assert_eq!(
            data.get_value_as_string(),
            "{\"email\":\"tiago@test.com\",\"id\":\"1234\",\"name\":\"Tiago\"}"
        );
    }
}
