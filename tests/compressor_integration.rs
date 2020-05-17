use rand::random;
use rustdb::{InitialSegmentReference, KeyValue, LogCompressor, RustDB};
use std::fs::{read_dir, remove_dir_all};

static STORAGE_TEST_FOLDER: &str = "storage_test";

fn folder_name() -> String {
    format!("{}{}", STORAGE_TEST_FOLDER, random::<u64>())
}

fn path_to_folder(path: &str) -> String {
    format!("./{}", path)
}

#[test]
fn compress_closed_files() {
    // arrange
    let path = &folder_name();
    let mut db = RustDB::load(path);

    for i in 0..200 {
        let id = i % 3;
        db.save_record(KeyValue::new_from_strings(
            format!("{:04}", id),
            format!("{{\"id\":\"{}\", \"name\":\"nome_{}\"}}", id, i),
        ))
        .unwrap();
    }

    // act
    let segment_names = db.get_closed_segment_names();
    let current_segment_name = db.segment.unwrap().name;
    let compressor = LogCompressor::new(path, segment_names, current_segment_name);

    let (active_segment, new_segment) = compressor.compress();

    // assert
    let reference = InitialSegmentReference::load(path);

    assert_eq!(current_segment_name, active_segment);
    assert_eq!(
        format!("{:016x}", current_segment_name),
        new_segment.next_segment_name.unwrap()
    );
    assert_eq!(reference.initial_segment.unwrap(), new_segment.name);

    remove_dir_all(path_to_folder(path)).unwrap();
}

#[test]
fn delete_compressed_files() {
    // arrange
    let path = &folder_name();
    let mut db = RustDB::load(path);

    for i in 0..200 {
        let id = i % 3;
        db.save_record(KeyValue::new_from_strings(
            format!("{:04}", id),
            format!("{{\"id\":\"{}\", \"name\":\"nome_{}\"}}", id, i),
        ))
        .unwrap();
    }

    // act
    let segment_names = db.get_closed_segment_names();
    let current_segment_name = db.segment.unwrap().name;
    let compressor = LogCompressor::new(path, segment_names.clone(), current_segment_name);

    let (_, new_segment) = compressor.compress();
    LogCompressor::clean(&path, segment_names);

    // assert
    let paths: Vec<String> = read_dir(path_to_folder(path))
        .unwrap()
        .map(|r| r.unwrap())
        .map(|r| String::from(r.file_name().to_str().unwrap()))
        .collect();

    assert_eq!(3, paths.len());
    assert!(paths.contains(&new_segment.get_name()));
    assert!(paths.contains(&String::from("initial_segment")));

    remove_dir_all(path_to_folder(path)).unwrap();
}

#[test]
fn compress_and_replace() {
    // arrange
    let path = &folder_name();
    let mut db = RustDB::load(path);

    for i in 0..200 {
        let id = i % 3;
        db.save_record(KeyValue::new_from_strings(
            format!("{:04}", id),
            format!("{{\"id\":\"{}\", \"name\":\"nome_{}\"}}", id, i),
        ))
        .unwrap();
    }

    // act
    let segment_names = db.get_closed_segment_names();
    let current_segment_name = db.get_active_segment_name();
    let compressor = LogCompressor::new(path, segment_names.clone(), current_segment_name);

    let (active_segment, new_segment) = compressor.compress();

    let new_segment_name = new_segment.name;
    db.replace_segments(active_segment, new_segment);
    LogCompressor::clean(&path, segment_names);

    // assert
    assert_eq!(active_segment, db.get_active_segment_name());
    assert_eq!(
        new_segment_name,
        db.segment.unwrap().get_previous().as_ref().unwrap().name
    );

    remove_dir_all(path_to_folder(path)).unwrap();
}
