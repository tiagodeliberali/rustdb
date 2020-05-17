use std::io::Result;

use crate::core::KeyValue;
use crate::store::{DataSgment, InitialSegmentReference};

#[cfg(test)]
static MAX_SIZE_FILE: u64 = 1_000_000;

#[cfg(not(test))]
static MAX_SIZE_FILE: u64 = 1_000;

pub struct RustDB {
    pub segment: Option<DataSgment>,
    folder: String,
}

impl RustDB {
    pub fn load(folder: &str) -> RustDB {
        RustDB {
            segment: Some(DataSgment::load_dir(folder)),
            folder: String::from(folder),
        }
    }

    fn new(folder: &str) -> RustDB {
        RustDB {
            segment: Some(DataSgment::new(folder)),
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

    pub fn get_closed_segment_names(&self) -> Vec<String> {
        let mut result = Vec::new();

        let seg = match &self.segment {
            Some(s) => s,
            None => return result,
        };

        if *seg.is_closed() {
            result.push(seg.get_name());
        }

        let mut previous = seg.get_previous();

        while let Some(s) = previous {
            result.push(s.get_name());
            previous = s.get_previous();
        }

        result
    }

    pub fn get_active_segment_name(&self) -> u64 {
        self.segment.as_ref().unwrap().name
    }

    pub fn replace_segments(&mut self, replace_segment: u64, new_segment: DataSgment) {
        RustDB::recursive(self.segment.as_mut().unwrap(), replace_segment, new_segment);
    }

    fn recursive(current_segment: &mut DataSgment, replace_segment: u64, new_segment: DataSgment) {
        if current_segment.name == replace_segment {
            current_segment.previous.replace(Box::from(new_segment));
        } else {
            if current_segment.previous.is_some() {
                RustDB::recursive(
                    current_segment.previous.as_mut().unwrap(),
                    replace_segment,
                    new_segment,
                );
            }
        }
    }
}

pub struct LogCompressor {
    db: RustDB,
    folder: String,
    closed_segments: Vec<String>,
    active_segment_name: u64,
}

impl LogCompressor {
    pub fn new(
        folder: &str,
        closed_segments: Vec<String>,
        active_segment_name: u64,
    ) -> LogCompressor {
        LogCompressor {
            db: RustDB::new(folder),
            folder: String::from(folder),
            closed_segments,
            active_segment_name,
        }
    }

    pub fn compress(mut self) -> (u64, DataSgment) {
        for segment_name in self.closed_segments {
            let data_segment = DataSgment::open(&format!("{}/{}", self.folder, segment_name));

            for key in data_segment.index.keys() {
                let key = String::from_utf8(key.clone()).unwrap();
                if self.db.get_record(key.clone()).unwrap().is_none() {
                    let key_value = data_segment.get_record(key).unwrap().unwrap();
                    self.db.save_record(key_value).unwrap();
                }
            }
        }

        let mut current_segment = self.db.segment.unwrap();
        let mut latest_segment_name = current_segment.name;
        let mut previous_segment = current_segment.get_previous();

        while let Some(seg) = previous_segment {
            latest_segment_name = seg.name;
            previous_segment = seg.get_previous();
        }

        current_segment.update_next_file(self.active_segment_name);

        let reference = InitialSegmentReference::load(&self.folder);
        reference.update(latest_segment_name);

        (self.active_segment_name, current_segment)
    }

    pub fn clean(folder: &str, segments: Vec<String>) {
        for segment_name in segments {
            DataSgment::remove(folder, &segment_name);
        }
    }
}
