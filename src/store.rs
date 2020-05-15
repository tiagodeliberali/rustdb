use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc::crc32;
use std::collections::HashMap;
use std::fs::{create_dir_all, read_dir, File, OpenOptions};
use std::io::{
    prelude::*, BufReader, Error, ErrorKind, ErrorKind::UnexpectedEof, Result, SeekFrom,
};
use std::path::Path;

use crate::core::{ByteString, KeyValue};

pub struct DataSgment {
    database_file: File,
    index: HashMap<ByteString, u64>,
    closed: bool,
    previous: Option<Box<DataSgment>>,
    size: u64,
    position: u64,
}

impl DataSgment {
    pub fn load_dir(folder: &str) -> DataSgment {
        create_dir_all(format!("./{}", folder)).unwrap();

        let mut paths: Vec<_> = read_dir(format!("./{}", folder))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        paths.sort_by_key(|dir| dir.file_name());

        let mut segment = None;
        for path in paths {
            let mut current = DataSgment::open(path.path().to_str().unwrap());
            segment = match segment {
                None => Some(current),
                Some(value) => {
                    current.set_previous(Some(value));
                    Some(current)
                }
            }
        }

        let position: u64 = match &segment {
            Some(s) => s.get_position() + 1,
            None => 1,
        };

        let mut current_segment = DataSgment::new(folder, position);

        if let Some(value) = segment {
            current_segment.previous.replace(Box::from(value));
        }

        current_segment
    }

    pub fn new(folder: &str, position: u64) -> DataSgment {
        create_dir_all(format!("./{}", folder)).unwrap();

        let database_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(Path::new(&format!("./{}/{:016x}", folder, position)))
            .unwrap();

        let mut buffer = BufReader::new(&database_file);
        let size = buffer.seek(SeekFrom::End(0)).unwrap();

        DataSgment {
            database_file,
            index: HashMap::new(),
            closed: false,
            previous: None,
            size,
            position,
        }
    }

    pub fn open(file_name: &str) -> DataSgment {
        let path = Path::new(file_name);

        let database_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(path)
            .unwrap();

        let mut buffer = BufReader::new(&database_file);
        let size = buffer.seek(SeekFrom::End(0)).unwrap();

        let mut segment = DataSgment {
            database_file,
            index: HashMap::new(),
            closed: true,
            previous: None,
            size,
            position: path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .to_string()
                .parse::<u64>()
                .unwrap(),
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
        index.insert(key_value.key.to_owned(), position);
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

    pub fn get_record(&self, key: String) -> Result<Option<KeyValue>> {
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

    pub fn delete_record(&mut self, key: String) -> Result<()> {
        let key: Vec<u8> = Vec::from(key);
        self.save_record(KeyValue::new(key, Vec::new()))?;
        Ok(())
    }

    pub fn save_record(&mut self, key_value: KeyValue) -> Result<()> {
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

    pub fn get_size(&self) -> u64 {
        self.size
    }

    pub fn get_position(&self) -> u64 {
        self.position
    }

    pub fn get_previous(&self) -> &Option<Box<DataSgment>> {
        &self.previous
    }

    pub fn set_previous(&mut self, segment: Option<DataSgment>) {
        if let Some(mut value) = segment {
            value.closed = true;
            self.previous.replace(Box::from(value));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::random;
    use std::fs::{copy, remove_dir_all};
    use std::{thread, time};

    fn get_folder_name() -> String {
        format!("storage_test_{}", random::<u64>())
    }

    #[test]
    fn create_empty_segment_on_new_db() {
        let path = &get_folder_name();

        let segment = DataSgment::new(path, 1);

        assert!(!segment.closed);
        assert_eq!(segment.size, 0);
        assert!(segment.previous.is_none());

        remove_dir_all(format!("./{}", path)).unwrap();
    }

    #[test]
    fn open_existing_segment() {
        let segment = DataSgment::open("./readonly_storage_test/1");

        assert!(segment.closed);
        assert_eq!(segment.size, 69);
        assert!(segment.previous.is_none());
    }

    #[test]
    fn update_size_on_save_data() {
        let path = &get_folder_name();

        let mut segment = DataSgment::new(path, 1);
        segment
            .save_record(KeyValue::new_from_strings(
                String::from("123"),
                String::from("{\"id\":\"123\",\"name\":\"test\"}"),
            ))
            .unwrap();

        assert!(!segment.closed);
        assert_eq!(segment.size, 41);
        assert!(segment.previous.is_none());

        remove_dir_all(format!("./{}", path)).unwrap();
    }

    #[test]
    fn load_segments() {
        let path = &get_folder_name();
        create_dir_all(format!("./{}", path)).unwrap();

        copy("./readonly_storage_test/1", format!("./{}/1", path)).unwrap();

        copy("./readonly_storage_test/2", format!("./{}/2", path)).unwrap();

        copy("./readonly_storage_test/3", format!("./{}/3", path)).unwrap();

        let segment = DataSgment::load_dir(path);

        // first segment is always a neew open one
        assert!(!segment.closed);
        assert_eq!(segment.get_size(), 0);
        assert!(segment.get_previous().is_some());

        let segment = match segment.get_previous() {
            None => {
                assert!(false);
                return ();
            }
            Some(value) => value,
        };

        assert!(segment.closed);
        assert_eq!(segment.get_size(), 154);
        assert!(segment.get_previous().is_some());

        let segment = match segment.get_previous() {
            None => {
                assert!(false);
                return ();
            }
            Some(value) => value,
        };

        assert!(segment.closed);
        assert_eq!(segment.get_size(), 136);
        assert!(segment.get_previous().is_some());

        let segment = match segment.get_previous() {
            None => {
                assert!(false);
                return ();
            }
            Some(value) => value,
        };

        assert!(segment.closed);
        assert_eq!(segment.get_size(), 69);
        assert!(segment.get_previous().is_none());

        remove_dir_all(format!("./{}", path)).unwrap();
    }

    #[test]
    fn load_files_in_order() {
        let path = &get_folder_name();
        create_dir_all(format!("./{}", path)).unwrap();

        File::create(format!("{}/3", path)).unwrap();
        thread::sleep(time::Duration::from_millis(10));

        File::create(format!("{}/1", path)).unwrap();
        thread::sleep(time::Duration::from_millis(10));

        File::create(format!("{}/2", path)).unwrap();

        let segment = DataSgment::load_dir(path);

        // ignore first file
        let segment = match segment.get_previous() {
            None => {
                assert!(false);
                return ();
            }
            Some(value) => value,
        };

        assert_eq!(segment.get_position(), 3);

        let segment = match segment.get_previous() {
            None => {
                assert!(false);
                return ();
            }
            Some(value) => value,
        };

        assert_eq!(segment.get_position(), 2);

        let segment = match segment.get_previous() {
            None => {
                assert!(false);
                return ();
            }
            Some(value) => value,
        };

        assert_eq!(segment.get_position(), 1);

        remove_dir_all(format!("./{}", path)).unwrap();
    }
}
