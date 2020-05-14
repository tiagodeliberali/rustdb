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
    pub fn load(folder: &str) -> RustDB {
        RustDB {
            segment: Some(DataSgment::load_dir(folder)),
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
            }
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
