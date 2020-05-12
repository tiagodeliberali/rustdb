use rand::random;
use rustdb::{KeyValue, RustDB};
use std::fs::{read_dir, remove_dir_all};

static STORAGE_TEST_FOLDER: &str = "storage_test";
static STORAGE_TEST_READONLY_FOLDER: &str = "./readonly_storage_test";
static STORAGE_TEST_FILE: &str = "./readonly_storage_test/integration_current_db";

static KEY: &str = "ABC";
static VALUE: &str = "{\"id\":\"ABC\",\"name\":\"Tiago\"}";

#[test]
fn open_existing_segment_and_find_record() {
    // arrange
    let db = RustDB::open(STORAGE_TEST_FILE);

    // act
    let data = db.get_record(String::from("1234"));

    // assert
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

#[test]
fn load_folder_and_find_all_records() {
    // arrange
    let db = RustDB::load(STORAGE_TEST_READONLY_FOLDER);

    // act
    let data1 = db.get_record(String::from("1234"));
    let data2 = db.get_record(String::from("1235"));
    let data3 = db.get_record(String::from("1236"));
    let data4 = db.get_record(String::from("1237"));

    // assert
    assert_eq!(data1.is_ok(), true);
    assert_eq!(data2.is_ok(), true);
    assert_eq!(data3.is_ok(), true);
    assert_eq!(data4.is_ok(), true);

    validate_value(
        data1.unwrap(),
        "{\"email\":\"tiago@test.com\",\"id\":\"1234\",\"name\":\"Tiago\"}",
    );
    validate_value(
        data2.unwrap(),
        "{\"email\":\"glau@test.com\",\"id\":\"1235\",\"name\":\"Glau\"}",
    );
    validate_value(
        data3.unwrap(),
        "{\"email\":\"alice_novo@test.com\",\"id\":\"1236\",\"name\":\"Alice atualizado\"}",
    );
    validate_value(
        data4.unwrap(),
        "{\"email\":\"lucas@test.com\",\"id\":\"1237\",\"name\":\"Lucas\"}",
    );
}

fn validate_value(result: Option<KeyValue>, content: &str) {
    if let Some(value) = result {
        assert_eq!(value.get_value_as_string(), content);
    } else {
        assert!(false, "result is empty");
    }
}

#[test]
fn open_new_file_and_add_item() {
    // arrange
    let path = &format!("{}{}", STORAGE_TEST_FOLDER, random::<u64>());

    let mut db = RustDB::new(path);
    let key_value = KeyValue::new_from_strings(String::from(KEY), String::from(VALUE));

    // act
    db.save_record(key_value).unwrap();

    // assert
    let data = db.get_record(String::from(KEY));
    assert_eq!(data.is_ok(), true);

    let data = data.unwrap();
    assert_eq!(data.is_some(), true);

    let data = data.unwrap();
    assert_eq!(data.get_key_as_string(), KEY);
    assert_eq!(data.get_value_as_string(), VALUE);

    remove_dir_all(format!("./{}", path)).unwrap();
}

#[test]
fn open_new_file_and_update_item() {
    // arrange
    let path = &format!("{}{}", STORAGE_TEST_FOLDER, random::<u64>());

    let updated_value =
        "{\"email\":\"tiago@test.com\",\"id\":\"1234\",\"name\":\"Tiago updated name\"}";

    let mut db = RustDB::new(path);
    let key_value_original = KeyValue::new_from_strings(String::from(KEY), String::from(VALUE));
    let key_value_updated =
        KeyValue::new_from_strings(String::from(KEY), String::from(updated_value));

    // act
    db.save_record(key_value_original).unwrap();
    db.save_record(key_value_updated).unwrap();

    // assert
    let data = db.get_record(String::from(KEY));
    assert_eq!(data.is_ok(), true);

    let data = data.unwrap();
    assert_eq!(data.is_some(), true);

    let data = data.unwrap();
    assert_eq!(data.get_key_as_string(), KEY);
    assert_eq!(data.get_value_as_string(), updated_value);

    remove_dir_all(format!("./{}", path)).unwrap();
}

#[test]
fn open_new_file_and_delete_item() {
    // arrange
    let path = &format!("{}{}", STORAGE_TEST_FOLDER, random::<u64>());

    let mut db = RustDB::new(path);
    let key_value = KeyValue::new_from_strings(String::from(KEY), String::from(VALUE));

    // act
    db.save_record(key_value).unwrap();
    db.delete_record(String::from(KEY)).unwrap();

    // assert
    let data = db.get_record(String::from(KEY));
    assert_eq!(data.is_ok(), true);

    let data = data.unwrap();
    assert_eq!(data.is_none(), true);

    remove_dir_all(format!("./{}", path)).unwrap();
}

#[test]
fn create_multiple_files() {
    // arrange
    let path = &format!("{}{}", STORAGE_TEST_FOLDER, random::<u64>());

    let mut db = RustDB::new(path);

    // act
    for i in 0..200 {
        db.save_record(KeyValue::new_from_strings(
            format!("{:04x}", i),
            format!(
                "{{\"email\":\"{}@test.com\",\"id\":\"{}\",\"name\":\"nome {}\"}}",
                i, i, i
            ),
        ))
        .unwrap();
    }

    // assert
    let paths = read_dir(path).unwrap();
    assert_eq!(13, paths.count());

    remove_dir_all(format!("./{}", path)).unwrap();
}

#[test]
fn read_from_multiple_files() {
    // arrange
    let path = &format!("{}{}", STORAGE_TEST_FOLDER, random::<u64>());

    let mut db = RustDB::new(path);

    // act
    for i in 0..80 {
        db.save_record(KeyValue::new_from_strings(
            format!("{:04}", i),
            format!(
                "{{\"email\":\"{}@test1.com\",\"id\":\"{}\",\"name\":\"nome {}\"}}",
                i, i, i
            ),
        ))
        .unwrap();
    }

    for i in 40..160 {
        db.save_record(KeyValue::new_from_strings(
            format!("{:04}", i),
            format!(
                "{{\"email\":\"{}@test2.com\",\"id\":\"{}\",\"name\":\"nome {}\"}}",
                i, i, i
            ),
        ))
        .unwrap();
    }

    for i in 60..120 {
        db.save_record(KeyValue::new_from_strings(
            format!("{:04}", i),
            format!(
                "{{\"email\":\"{}@test3.com\",\"id\":\"{}\",\"name\":\"nome {}\"}}",
                i, i, i
            ),
        ))
        .unwrap();
    }

    // assert
    let data1 = db.get_record(String::from("0001"));
    let data2 = db.get_record(String::from("0050"));
    let data3 = db.get_record(String::from("0080"));
    let data4 = db.get_record(String::from("0130"));

    validate_value(
        data1.unwrap(),
        "{\"email\":\"1@test1.com\",\"id\":\"1\",\"name\":\"nome 1\"}",
    );
    validate_value(
        data2.unwrap(),
        "{\"email\":\"50@test2.com\",\"id\":\"50\",\"name\":\"nome 50\"}",
    );
    validate_value(
        data3.unwrap(),
        "{\"email\":\"80@test3.com\",\"id\":\"80\",\"name\":\"nome 80\"}",
    );
    validate_value(
        data4.unwrap(),
        "{\"email\":\"130@test2.com\",\"id\":\"130\",\"name\":\"nome 130\"}",
    );

    remove_dir_all(format!("./{}", path)).unwrap();
}
