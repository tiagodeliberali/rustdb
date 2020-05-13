use std::fs::read_dir;
use std::io::Result;

use crate::core::KeyValue;
use crate::store::DataSgment;

#[cfg(test)]
static MAX_SIZE_FILE: u64 = 1_000_000;

#[cfg(not(test))]
static MAX_SIZE_FILE: u64 = 1_000;

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
                    current.set_previous(Some(value));
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
            Some(value) => {
                let result = self.get_record_from_segment(&key, value)?;
                if let Some(v) = &result {
                    if v.get_value_as_string().len() == 0 {
                        return Ok(None);
                    }
                    return Ok(result);
                }
                return Ok(None);
            },
            None => return Ok(None),
        }
    }

    fn get_record_from_segment(&self, key: &str, segment: &DataSgment) -> Result<Option<KeyValue>> {
        let record = segment.get_record(String::from(key))?;

        match record {
            Some(_) => return Ok(record),
            None => {
                if let Some(next) = segment.get_previous() {
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

                if value.get_size() > MAX_SIZE_FILE {
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

    static STORAGE_TEST_READONLY_FOLDER: &str = "./readonly_storage_test";

    #[test]
    fn load_segments() {
        let db = RustDB::load(STORAGE_TEST_READONLY_FOLDER);

        let segment = &db.segment.unwrap();

        assert!(segment.is_closed());
        assert_eq!(segment.get_size(), 154);
        assert!(segment.get_previous().is_some());

        let segment = match segment.get_previous() {
            None => {
                assert_eq!(1, 0);
                return ();
            }
            Some(value) => value,
        };

        assert!(segment.is_closed());
        assert_eq!(segment.get_size(), 136);
        assert!(segment.get_previous().is_some());

        let segment = match segment.get_previous() {
            None => {
                assert_eq!(1, 0);
                return ();
            }
            Some(value) => value,
        };

        assert!(segment.is_closed());
        assert_eq!(segment.get_size(), 69);
        assert!(segment.get_previous().is_none());
    }
}
