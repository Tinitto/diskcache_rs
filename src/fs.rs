use core::fmt::Debug;
use core::option::Option;
use core::option::Option::{None, Some};
use core::result::Result::{Err, Ok};
use tokio::{fs, io};

pub(crate) async fn save_to_file(store_path: &str, key: &String, value: &String) -> Option<String> {
    let file_path = format!("{}/{}", store_path, key);
    let result = fs::write(file_path, value.clone()).await;
    result_to_option(result)
}

pub(crate) async fn get_from_file(store_path: &str, key: &String) -> Option<String> {
    let file_path = format!("{}/{}", store_path, key);
    let result = fs::read_to_string(file_path).await;
    result_to_option(result)
}

pub(crate) async fn remove_from_file(store_path: &str, key: &String) -> Option<String> {
    let file_path = format!("{}/{}", store_path, key);
    let result = fs::remove_file(file_path).await;
    result_to_option(result)
}

pub(crate) async fn clear_from_file(store_path: &str) -> Option<String> {
    let _ = fs::remove_dir_all(store_path).await;
    None
}

pub(crate) fn initialize_file_db(store_path: &str) {
    let _ = std::fs::create_dir_all(store_path);
}

fn result_to_option<T: Debug>(result: io::Result<T>) -> Option<String> {
    match result {
        Ok(t) => Some(format!("{:?}", t)),
        Err(..) => None,
    }
}
