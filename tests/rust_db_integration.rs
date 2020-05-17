use rand::random;
use rustdb::{KeyValue, RustDB};
use std::fs::{copy, create_dir_all, read_dir, remove_dir_all};

static STORAGE_TEST_FOLDER: &str = "storage_test";

static KEY: &str = "ABC";
static VALUE: &str = "{\"id\":\"ABC\",\"name\":\"Tiago\"}";

fn folder_name() -> String {
    format!("{}{}", STORAGE_TEST_FOLDER, random::<u64>())
}

fn copy_read_only_files(folder_name: &str) {
    create_dir_all(format!("./{}", folder_name)).unwrap();
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
}

#[test]
fn open_existing_segment_and_find_record() {
    // arrange
    let path = &folder_name();
    copy_read_only_files(path);
    let db = RustDB::load(path);

    // act
    let data = db.get_record(String::from("0001"));

    // assert
    assert!(data.is_ok());

    let data = data.unwrap();
    assert!(data.is_some());

    let data = data.unwrap();
    assert_eq!(data.get_key_as_string(), "0001");
    assert_eq!(
        data.get_value_as_string(),
        "{\"email\":\"1@test1.com\",\"id\":\"1\",\"name\":\"nome 1\"}"
    );

    remove_dir_all(format!("./{}", path)).unwrap();
}

#[test]
fn load_folder_and_find_all_records() {
    // arrange
    let path = &folder_name();
    copy_read_only_files(path);

    let db = RustDB::load(path);

    // act
    let data1 = db.get_record(String::from("0028"));
    let data2 = db.get_record(String::from("0015"));
    let data3 = db.get_record(String::from("0008"));
    let data4 = db.get_record(String::from("0034"));

    // assert
    assert!(data1.is_ok());
    assert!(data2.is_ok());
    assert!(data3.is_ok());
    assert!(data4.is_ok());

    validate_value(
        data1.unwrap(),
        "{\"email\":\"28@test1.com\",\"id\":\"28\",\"name\":\"nome 28\"}",
    );
    validate_value(
        data2.unwrap(),
        "{\"email\":\"15@test1.com\",\"id\":\"15\",\"name\":\"nome 15\"}",
    );
    validate_value(
        data3.unwrap(),
        "{\"email\":\"8@test1.com\",\"id\":\"8\",\"name\":\"nome 8\"}",
    );
    validate_value(
        data4.unwrap(),
        "{\"email\":\"34@test1.com\",\"id\":\"34\",\"name\":\"nome 34\"}",
    );

    remove_dir_all(format!("./{}", path)).unwrap();
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
    let path = &folder_name();

    let mut db = RustDB::load(path);
    let key_value = KeyValue::new_from_strings(String::from(KEY), String::from(VALUE));

    // act
    db.save_record(key_value).unwrap();

    // assert
    let data = db.get_record(String::from(KEY));
    assert!(data.is_ok());

    let data = data.unwrap();
    assert!(data.is_some());

    let data = data.unwrap();
    assert_eq!(data.get_key_as_string(), KEY);
    assert_eq!(data.get_value_as_string(), VALUE);

    remove_dir_all(format!("./{}", path)).unwrap();
}

#[test]
fn open_new_file_and_update_item() {
    // arrange
    let path = &&folder_name();

    let updated_value =
        "{\"email\":\"tiago@test.com\",\"id\":\"1234\",\"name\":\"Tiago updated name\"}";

    let mut db = RustDB::load(path);
    let key_value_original = KeyValue::new_from_strings(String::from(KEY), String::from(VALUE));
    let key_value_updated =
        KeyValue::new_from_strings(String::from(KEY), String::from(updated_value));

    // act
    db.save_record(key_value_original).unwrap();
    db.save_record(key_value_updated).unwrap();

    // assert
    let data = db.get_record(String::from(KEY));
    assert!(data.is_ok());

    let data = data.unwrap();
    assert!(data.is_some());

    let data = data.unwrap();
    assert_eq!(data.get_key_as_string(), KEY);
    assert_eq!(data.get_value_as_string(), updated_value);

    remove_dir_all(format!("./{}", path)).unwrap();
}

#[test]
fn open_new_file_and_delete_item() {
    // arrange
    let path = &&folder_name();

    let mut db = RustDB::load(path);
    let key_value = KeyValue::new_from_strings(String::from(KEY), String::from(VALUE));

    // act
    db.save_record(key_value).unwrap();
    db.delete_record(String::from(KEY)).unwrap();

    // assert
    let data = db.get_record(String::from(KEY));
    assert!(data.is_ok());

    let data = data.unwrap();
    assert!(data.is_none());

    remove_dir_all(format!("./{}", path)).unwrap();
}

#[test]
fn create_multiple_files() {
    // arrange
    let path = &&folder_name();

    let mut db = RustDB::load(path);

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
    assert_eq!(15, paths.count());

    remove_dir_all(format!("./{}", path)).unwrap();
}

#[test]
fn read_from_multiple_files() {
    // arrange
    let path = &&folder_name();

    let mut db = RustDB::load(path);

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

#[test]
fn delete_item_that_exists_on_previous_segment() {
    // arrange
    let path = &&folder_name();

    let mut db = RustDB::load(path);

    // create enough records to have more than on file
    for i in 0..30 {
        db.save_record(KeyValue::new_from_strings(
            format!("{:04}", i),
            format!(
                "{{\"email\":\"{}@test.com\",\"id\":\"{}\",\"name\":\"nome {}\"}}",
                i, i, i
            ),
        ))
        .unwrap();
    }

    // remove a record from first file
    db.delete_record(String::from("0001")).unwrap();

    // act
    let result = db.get_record(String::from("0001")).unwrap();

    // assert
    let paths = read_dir(path).unwrap();
    assert!(paths.count() > 1); // check if we have more than on file

    assert!(result.is_none());

    remove_dir_all(format!("./{}", path)).unwrap();
}

#[test]
fn get_closed_segment_names() {
    // arrange
    let path = &folder_name();
    copy_read_only_files(path);
    let db = RustDB::load(path);

    // act
    let data: Vec<String> = db.get_closed_segment_names();

    // assert
    assert_eq!(data.len(), 3);

    assert_eq!(data.get(0).unwrap(), "4da053f2db81bb26");
    assert_eq!(data.get(1).unwrap(), "e0c515663f0ea931");
    assert_eq!(data.get(2).unwrap(), "53e155bcbdeb560f");

    remove_dir_all(format!("./{}", path)).unwrap();
}
