use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc::crc32;
use rand::random;
use std::collections::HashMap;
use std::fs::{create_dir_all, read_dir, File, OpenOptions};
use std::io::{
    prelude::*, BufReader, Error, ErrorKind, ErrorKind::UnexpectedEof, Result, SeekFrom,
};
use std::path::Path;

#[cfg(test)]
static MAX_SIZE_FILE: u64 = 1_000_000;

#[cfg(not(test))]
static MAX_SIZE_FILE: u64 = 1_000;

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
        create_dir_all(format!("./{}", folder)).unwrap();

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

        self.size = self.database_file.seek(SeekFrom::End(0))?;

        Ok(())
    }

    fn set_previous(&mut self, segment: Option<DataSgment>) {
        if let Some(mut value) = segment {
            value.closed = true;
            self.previous.replace(Box::from(value));
        }
    }
}

pub struct RustDB {
    segment: Option<DataSgment>,
    folder: String,
}

impl RustDB {
    pub fn open(file_name: &str) -> RustDB {
        RustDB {
            segment: Some(DataSgment::open(file_name)),
            folder: String::from(file_name),
        }
    }

    pub fn new(folder: &str) -> RustDB {
        RustDB {
            segment: Some(DataSgment::new(folder)),
            folder: String::from(folder),
        }
    }

    pub fn load(folder: &str) -> RustDB {
        let mut paths: Vec<_> = read_dir(format!("./{}", folder))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        paths.sort_by_key(|dir| dir.metadata().unwrap().created().unwrap());

        let mut segment = None;
        for path in paths {
            let mut current = DataSgment::open(path.path().to_str().unwrap());
            segment = match segment {
                None => Some(current),
                Some(value) => {
                    current.previous = Some(Box::from(value));
                    Some(current)
                }
            }
        }

        let segment = match segment {
            None => DataSgment::new(folder),
            Some(value) => value,
        };

        RustDB {
            segment: Some(segment),
            folder: String::from(folder),
        }
    }

    pub fn get_record(&self, key: String) -> Result<Option<KeyValue>> {
        match &self.segment {
            Some(value) => self.get_record_from_segment(&key, value),
            None => return Ok(None),
        }
    }

    fn get_record_from_segment(&self, key: &str, segment: &DataSgment) -> Result<Option<KeyValue>> {
        let record = segment.get_record(String::from(key))?;

        match record {
            Some(_) => return Ok(record),
            None => {
                if let Some(next) = &segment.previous {
                    return self.get_record_from_segment(key, &next);
                }
                return Ok(None);
            }
        }
    }

    pub fn delete_record(&mut self, key: String) -> Result<()> {
        match &mut self.segment {
            Some(value) => value.delete_record(key),
            None => return Ok(()),
        }
    }

    pub fn save_record(&mut self, key_value: KeyValue) -> Result<()> {
        match &mut self.segment {
            Some(value) => {
                value.save_record(key_value)?;

                if value.size > MAX_SIZE_FILE {
                    let new_segment = DataSgment::new(&self.folder);
                    let current_segment = self.segment.replace(new_segment);
                    self.segment.as_mut().unwrap().set_previous(current_segment);
                }
            }
            None => return Ok(()),
        };

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::remove_dir_all;

    static STORAGE_TEST_FOLDER: &str = "storage_test_";
    static STORAGE_TEST_READONLY_FOLDER: &str = "./readonly_storage_test";
    static STORAGE_TEST_FILE: &str = "./readonly_storage_test/integration_current_db";

    #[test]
    fn create_empty_segment_on_new_db() {
        let path = &format!("{}{}", STORAGE_TEST_FOLDER, random::<u64>());

        let db = RustDB::new(path);

        let segment = db.segment.unwrap();

        assert_eq!(segment.closed, false);
        assert_eq!(segment.size, 0);
        assert_eq!(segment.previous.is_none(), true);

        remove_dir_all(format!("./{}", path)).unwrap();
    }

    #[test]
    fn open_existing_segment() {
        let db = RustDB::open(STORAGE_TEST_FILE);

        let segment = db.segment.unwrap();

        assert_eq!(segment.closed, true);
        assert_eq!(segment.size, 69);
        assert_eq!(segment.previous.is_none(), true);
    }

    #[test]
    fn update_size_on_save_data() {
        let path = &format!("{}{}", STORAGE_TEST_FOLDER, random::<u64>());

        let mut db = RustDB::new(path);
        db.save_record(KeyValue::new_from_strings(
            String::from("123"),
            String::from("{\"id\":\"123\",\"name\":\"test\"}"),
        ))
        .unwrap();

        let segment = db.segment.unwrap();

        assert_eq!(segment.closed, false);
        assert_eq!(segment.size, 41);
        assert_eq!(segment.previous.is_none(), true);

        remove_dir_all(format!("./{}", path)).unwrap();
    }

    #[test]
    fn load_segments() {
        let db = RustDB::load(STORAGE_TEST_READONLY_FOLDER);

        let segment = &db.segment.unwrap();

        assert_eq!(segment.closed, true);
        assert_eq!(segment.size, 154);
        assert_eq!(segment.previous.is_some(), true);

        let segment = match &segment.previous {
            None => {
                assert_eq!(1, 0);
                return ();
            }
            Some(value) => value,
        };

        assert_eq!(segment.closed, true);
        assert_eq!(segment.size, 136);
        assert_eq!(segment.previous.is_some(), true);

        let segment = match &segment.previous {
            None => {
                assert_eq!(1, 0);
                return ();
            }
            Some(value) => value,
        };

        assert_eq!(segment.closed, true);
        assert_eq!(segment.size, 69);
        assert_eq!(segment.previous.is_none(), true);
    }
}
