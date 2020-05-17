use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use crc::crc32;
use rand::random;
use std::collections::HashMap;
use std::fs::{create_dir_all, File, OpenOptions};
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
    name: u64,
    next_segment_name: Option<String>,
}

fn folder_path(folder_name: &str) -> String {
    format!("./{}", folder_name)
}

fn initial_segment_file(folder_name: &str) -> String {
    format!("{}/initial_segment", folder_path(folder_name))
}

fn read_initial_segment_reference(folder_name: &str) -> Result<u64> {
    let mut initial_segment = File::open(initial_segment_file(&folder_name))?;
    let name = initial_segment.read_u64::<BigEndian>().unwrap();

    Ok(name)
}

fn parse_file_name(name: u64) -> String {
    format!("{:016x}", name)
}

fn parse_next_segment_name(next_segment_name: u64) -> Option<String> {
    match next_segment_name {
        0_u64 => None,
        _ => Some(parse_file_name(next_segment_name)),
    }
}

fn build_path(folder_path: &str, file: &str) -> String {
    format!("{}/{}", folder_path, file)
}

fn create_initial_segment_reference(folder_path: &str, name: u64) {
    let mut reference = File::create(Path::new(&initial_segment_file(folder_path))).unwrap();
    reference.write_u64::<BigEndian>(name).unwrap();
}

impl DataSgment {
    fn update_next_file(&mut self, name: u64) {
        self.database_file.seek(SeekFrom::Start(8)).unwrap();
        self.database_file.write_u64::<BigEndian>(name).unwrap();
        self.next_segment_name.replace(parse_file_name(name));
    }

    pub fn load_dir(folder: &str) -> DataSgment {
        let folder_path = folder_path(folder);
        create_dir_all(&folder_path).unwrap();

        let mut data_segment_name = match read_initial_segment_reference(folder) {
            Ok(value) => Some(parse_file_name(value)),
            Err(_) => {
                let new_segment = DataSgment::new(folder);
                create_initial_segment_reference(&folder_path, new_segment.name);
                return new_segment;
            }
        };

        let mut loaded_segment = None;
        while let Some(next) = &data_segment_name {
            let mut current = DataSgment::open(&build_path(&folder_path, next));

            data_segment_name = match &current.next_segment_name {
                None => None,
                Some(v) => Some(format!("{}", v)),
            };

            loaded_segment = match loaded_segment {
                None => Some(current),
                Some(s) => {
                    current.previous.replace(Box::new(s));
                    Some(current)
                }
            };
        }

        let mut current_segment = DataSgment::new(folder);

        if let Some(mut value) = loaded_segment {
            value.update_next_file(current_segment.name);
            current_segment.previous.replace(Box::from(value));
        }

        current_segment
    }

    pub fn new(folder: &str) -> DataSgment {
        let folder_path = folder_path(folder);
        create_dir_all(&folder_path).unwrap();

        let name = random::<u64>();

        let mut database_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(Path::new(&build_path(&folder_path, &parse_file_name(name))))
            .unwrap();

        database_file.write_u64::<BigEndian>(name).unwrap();
        database_file.write_u64::<BigEndian>(0).unwrap();

        let size = database_file.seek(SeekFrom::End(0)).unwrap();

        DataSgment {
            database_file,
            index: HashMap::new(),
            closed: false,
            previous: None,
            size,
            name,
            next_segment_name: None,
        }
    }

    pub fn open(file_name: &str) -> DataSgment {
        let path = Path::new(file_name);

        let mut database_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .unwrap();

        database_file.seek(SeekFrom::Start(0)).unwrap();

        let name = database_file.read_u64::<BigEndian>().unwrap();

        let next_segment_name = database_file.read_u64::<BigEndian>().unwrap();

        let size = database_file.seek(SeekFrom::End(0)).unwrap();

        let mut segment = DataSgment {
            database_file,
            index: HashMap::new(),
            closed: true,
            previous: None,
            size,
            name,
            next_segment_name: parse_next_segment_name(next_segment_name),
        };

        segment.load().unwrap();

        segment
    }

    fn load(&mut self) -> Result<()> {
        let mut database_buffer = BufReader::new(&self.database_file);
        let _ = database_buffer.seek(SeekFrom::Start(16))?;

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
        let checksum = file.read_u32::<BigEndian>()?;
        let key_size: usize = file.read_u32::<BigEndian>()? as usize;
        let value_size: usize = file.read_u32::<BigEndian>()? as usize;
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

        self.database_file.write_u32::<BigEndian>(checksum)?;
        self.database_file.write_u32::<BigEndian>(key_size)?;
        self.database_file.write_u32::<BigEndian>(value_size)?;
        self.database_file.write(&data)?;

        DataSgment::update_index(&mut self.index, &key_value, position);

        self.size = self.database_file.seek(SeekFrom::End(0))?;

        Ok(())
    }

    pub fn get_size(&self) -> u64 {
        self.size
    }

    pub fn get_previous(&self) -> &Option<Box<DataSgment>> {
        &self.previous
    }

    pub fn is_closed(&self) -> &bool {
        &self.closed
    }

    pub fn set_previous(&mut self, segment: Option<DataSgment>) {
        if let Some(mut value) = segment {
            value.closed = true;
            value.database_file.seek(SeekFrom::Start(8)).unwrap();
            value.update_next_file(self.name);

            self.previous.replace(Box::from(value));
        }
    }

    pub fn get_name(&self) -> String {
        parse_file_name(self.name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{copy, read_dir, remove_dir_all};

    fn get_folder_name() -> String {
        format!("storage_test_{}", random::<u64>())
    }

    #[test]
    fn create_empty_segment_on_new_db() {
        let folder_name = &get_folder_name();

        let segment = DataSgment::new(folder_name);

        assert!(!segment.closed);
        assert_eq!(segment.size, 16);
        assert!(segment.previous.is_none());

        remove_dir_all(folder_path(folder_name)).unwrap();
    }

    #[test]
    fn open_existing_segment() {
        let segment = DataSgment::open("./readonly_storage_test/53e155bcbdeb560f");

        assert!(segment.closed);
        assert_eq!(segment.size, 1058);
        assert!(segment.previous.is_none());
    }

    #[test]
    fn update_size_on_save_data() {
        let folder_name = &get_folder_name();

        let mut segment = DataSgment::new(folder_name);
        segment
            .save_record(KeyValue::new_from_strings(
                String::from("123"),
                String::from("{\"id\":\"123\",\"name\":\"test\"}"),
            ))
            .unwrap();

        assert!(!segment.closed);
        assert_eq!(segment.size, 57);
        assert!(segment.previous.is_none());

        remove_dir_all(folder_path(folder_name)).unwrap();
    }

    #[test]
    fn load_segments() {
        let folder_name = &get_folder_name();
        create_dir_all(folder_path(folder_name)).unwrap();

        copy(
            "./readonly_storage_test/53e155bcbdeb560f",
            format!("./{}/53e155bcbdeb560f", folder_name),
        )
        .unwrap();
        copy(
            "./readonly_storage_test/4da053f2db81bb26",
            format!("./{}/4da053f2db81bb26", folder_name),
        )
        .unwrap();
        copy(
            "./readonly_storage_test/e0c515663f0ea931",
            format!("./{}/e0c515663f0ea931", folder_name),
        )
        .unwrap();
        copy(
            "./readonly_storage_test/initial_segment",
            format!("./{}/initial_segment", folder_name),
        )
        .unwrap();

        let segment = DataSgment::load_dir(folder_name);

        // first segment is always a neew open one
        assert!(!segment.closed);
        assert_eq!(segment.get_size(), 16);
        assert!(segment.next_segment_name.is_none());
        assert!(segment.get_previous().is_some());

        let name = parse_file_name(segment.name);
        let segment = match segment.get_previous() {
            None => {
                assert!(false);
                return ();
            }
            Some(value) => value,
        };

        assert!(segment.closed);
        assert_eq!(segment.get_size(), 619);
        assert!(&segment.next_segment_name.is_some());
        assert_eq!(segment.next_segment_name.as_ref().unwrap(), &name);
        assert!(segment.get_previous().is_some());

        let name = parse_file_name(segment.name);
        let segment = match segment.get_previous() {
            None => {
                assert!(false);
                return ();
            }
            Some(value) => value,
        };

        assert!(segment.closed);
        assert_eq!(segment.get_size(), 1021);
        assert!(&segment.next_segment_name.is_some());
        assert_eq!(segment.next_segment_name.as_ref().unwrap(), &name);
        assert!(segment.get_previous().is_some());

        let name = parse_file_name(segment.name);
        let segment = match segment.get_previous() {
            None => {
                assert!(false);
                return ();
            }
            Some(value) => value,
        };

        assert!(segment.closed);
        assert_eq!(segment.get_size(), 1058);
        assert!(&segment.next_segment_name.is_some());
        assert_eq!(segment.next_segment_name.as_ref().unwrap(), &name);
        assert!(segment.get_previous().is_none());

        remove_dir_all(folder_path(folder_name)).unwrap();
    }

    #[test]
    fn load_empty_dir_create_reference_to_first() {
        // arrange
        let folder_name = &get_folder_name();

        // act
        let segment = DataSgment::load_dir(folder_name);

        // assert
        let paths: Vec<String> = read_dir(folder_path(folder_name))
            .unwrap()
            .map(|r| r.unwrap())
            .map(|r| String::from(r.file_name().to_str().unwrap()))
            .collect();

        assert!(paths.contains(&parse_file_name(segment.name)));
        assert!(paths.contains(&String::from("initial_segment")));

        assert_eq!(
            segment.name,
            read_initial_segment_reference(folder_name).unwrap()
        );

        remove_dir_all(folder_path(folder_name)).unwrap();
    }
}
