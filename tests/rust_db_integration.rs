use rustdb::{KeyValue, RustDB};

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

    assert_eq!(data1.unwrap().is_some(), true);
    assert_eq!(data2.unwrap().is_some(), true);
    assert_eq!(data3.unwrap().is_some(), true);
    assert_eq!(data4.unwrap().is_some(), true);

    // assert_eq!(
    //     data1.unwrap().unwrap().get_value_as_string(),
    //     "{\"email\":\"tiago@test.com\",\"id\":\"1234\",\"name\":\"Tiago\"}"
    // );
}

#[test]
fn open_new_file_and_add_item() {
    // arrange
    let mut db = RustDB::new(STORAGE_TEST_FOLDER);
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
}

#[test]
fn open_new_file_and_update_item() {
    // arrange
    let updated_value =
        "{\"email\":\"tiago@test.com\",\"id\":\"1234\",\"name\":\"Tiago updated name\"}";

    let mut db = RustDB::new(STORAGE_TEST_FOLDER);
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
}

#[test]
fn open_new_file_and_delete_item() {
    // arrange
    let mut db = RustDB::new(STORAGE_TEST_FOLDER);
    let key_value = KeyValue::new_from_strings(String::from(KEY), String::from(VALUE));

    // act
    db.save_record(key_value).unwrap();
    db.delete_record(String::from(KEY)).unwrap();

    // assert
    let data = db.get_record(String::from(KEY));
    assert_eq!(data.is_ok(), true);

    let data = data.unwrap();
    assert_eq!(data.is_none(), true);
}
