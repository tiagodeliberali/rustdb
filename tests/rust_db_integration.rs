use rustdb::RustDB;

static STORAGE_TEST_FILE: &str = "./storage_test/current_db";

#[test]
fn open_existing_segment_and_find_record() {
    let db = RustDB::open(STORAGE_TEST_FILE);

    let data = db.get_record(String::from("1234"));
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
