use core::fmt::Debug;
use core::option::Option;
use core::option::Option::{None, Some};
use core::result::Result::{Err, Ok};
use std::{fs, io};

pub(crate) fn save_to_file(store_path: &str, key: &String, value: &String) -> Option<String> {
    let file_path = format!("{}/{}", store_path, key);
    let result = fs::write(file_path, value.clone());
    result_to_option(result)
}

pub(crate) fn get_from_file(store_path: &str, key: &String) -> Option<String> {
    let file_path = format!("{}/{}", store_path, key);
    let result = fs::read_to_string(file_path);
    result_to_option(result)
}

pub(crate) fn remove_from_file(store_path: &str, key: &String) -> Option<String> {
    let file_path = format!("{}/{}", store_path, key);
    let result = fs::remove_file(file_path);
    result_to_option(result)
}

pub(crate) fn clear_from_file(store_path: &str) -> Option<String> {
    let _ = fs::remove_dir_all(store_path);
    None
}

pub(crate) fn initialize_file_db(store_path: &str) {
    let _ = fs::create_dir_all(store_path);
}

fn result_to_option<T: Debug>(result: io::Result<T>) -> Option<String> {
    match result {
        Ok(t) => Some(format!("{:?}", t)),
        Err(..) => None,
    }
}
