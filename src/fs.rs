use std::io::ErrorKind::NotFound;
use tokio::{fs, io};

pub(crate) async fn save_to_file(store_path: &str, key: &String, value: &String) -> io::Result<()> {
    let file_path = format!("{}/{}", store_path, key);
    fs::write(file_path, value.clone()).await
}

pub(crate) async fn get_from_file(store_path: &str, key: &String) -> io::Result<Option<String>> {
    let file_path = format!("{}/{}", store_path, key);
    let result = fs::read_to_string(file_path).await;

    match result {
        Ok(value) => Ok(Some(value)),
        Err(_) => Ok(None),
    }
}

pub(crate) async fn remove_from_file(store_path: &str, key: &String) -> io::Result<()> {
    let file_path = format!("{}/{}", store_path, key);
    fs::remove_file(file_path).await
}

pub(crate) async fn clear_from_file(store_path: &str) -> io::Result<()> {
    if let Err(e) = fs::remove_dir_all(store_path).await {
        if e.kind() != NotFound {
            return Err(e);
        }
    };

    Ok(())
}

pub(crate) fn initialize_file_db(store_path: &str) {
    let _ = std::fs::create_dir_all(store_path);
}
