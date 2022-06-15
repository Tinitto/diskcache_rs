use core::option::Option::{None, Some};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};

pub enum Action {
    Set {
        key: String,
        value: String,
        resp: oneshot::Sender<io::Result<Option<String>>>,
    },
    Get {
        key: String,
        resp: oneshot::Sender<io::Result<Option<String>>>,
    },
    Del {
        key: String,
        resp: oneshot::Sender<io::Result<Option<String>>>,
    },
    Clear {
        resp: oneshot::Sender<io::Result<()>>,
    },
}

pub struct Store {
    db: Arc<Mutex<HashMap<String, String>>>,
    handlers: Vec<JoinHandle<()>>,
    store_path: String,
    receiver_mutex_arc: Arc<Mutex<mpsc::Receiver<Action>>>,
}

impl Store {
    pub(crate) fn new(
        receiver: mpsc::Receiver<Action>,
        num_of_handlers: usize,
        store_path: &str,
    ) -> Store {
        assert!(num_of_handlers > 1);

        let mut store = Store {
            db: Arc::new(Mutex::new(HashMap::new())),
            handlers: Vec::with_capacity(num_of_handlers),
            store_path: store_path.to_string(),
            receiver_mutex_arc: Arc::new(Mutex::new(receiver)),
        };

        crate::fs::initialize_file_db(&store.store_path);
        store.generate_handlers(num_of_handlers);
        store
    }

    pub async fn close(&self) {
        for handler in &self.handlers {
            handler.abort()
        }

        'outer: loop {
            sleep(Duration::from_millis(200)).await;

            for handler in &self.handlers {
                if !handler.is_finished() {
                    continue 'outer;
                }
            }

            break;
        }

        let receiver_mutex = Arc::clone(&self.receiver_mutex_arc);
        receiver_mutex.lock().await.close();
    }

    fn generate_handlers(&mut self, num_of_handlers: usize) {
        for _ in 0..num_of_handlers {
            let db_mutex = Arc::clone(&self.db);
            let receiver_mutex = Arc::clone(&self.receiver_mutex_arc);
            let store_path = self.store_path.clone();

            let handler = tokio::spawn(async move {
                loop {
                    let mut rv = receiver_mutex.lock().await;
                    let mut db = db_mutex.lock().await;
                    let action = rv.recv().await.unwrap();

                    match action {
                        Action::Set { key, value, resp } => {
                            let file_io = crate::fs::save_to_file(&store_path, &key, &value).await;
                            match file_io {
                                Err(v) => resp.send(Err(v)).unwrap(),
                                Ok(()) => {
                                    resp.send(Ok(db.insert(key, value))).unwrap();
                                }
                            }
                        }
                        Action::Get { key, resp } => {
                            let value = match db.get(&key[..]) {
                                Some(v) => Ok(Some(v.to_string())),
                                None => crate::fs::get_from_file(&store_path, &key).await,
                            };

                            resp.send(value).unwrap()
                        }
                        Action::Del { key, resp } => {
                            let file_io = crate::fs::remove_from_file(&store_path, &key).await;
                            match file_io {
                                Err(v) => resp.send(Err(v)).unwrap(),
                                Ok(()) => {
                                    let value = db.remove(&key[..]);
                                    resp.send(Ok(value)).unwrap();
                                }
                            }
                        }
                        Action::Clear { resp } => {
                            let file_io = crate::fs::clear_from_file(&store_path).await;
                            let value = match file_io {
                                Ok(()) => Ok(db.clear()),
                                Err(err) => Err(err),
                            };
                            resp.send(value).unwrap()
                        }
                    };
                }
            });

            self.handlers.push(handler);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use tokio::sync::mpsc::Sender;

    const STORE_PATH: &str = "db";
    const KEYS: [&str; 4] = ["hey", "hi", "yoo-hoo", "bonjour"];
    const VALUES: [&str; 4] = ["English", "English", "Slang", "French"];

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn set_and_read_multiple_key_value_pairs() {
        let (tx, rv) = mpsc::channel(1);
        let _store = Store::new(rv, 2, STORE_PATH);

        let keys = KEYS.to_vec();
        let values = VALUES.to_vec();

        insert_test_data(&tx, &keys, &values).await;
        let received_values = get_values_for_keys(&tx, keys).await;

        let expected_values: Vec<io::Result<Option<String>>> = values
            .into_iter()
            .map(|v| Ok(Some(v.to_string())))
            .collect();

        for (got, expected) in received_values.into_iter().zip(expected_values) {
            assert_eq!(got.unwrap(), expected.unwrap());
        }

        _store.close().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn set_and_delete_multiple_key_value_pairs() {
        let (tx, rv) = mpsc::channel(1);
        let _store = Store::new(rv, 2, STORE_PATH);

        let keys = KEYS.to_vec();
        let values = VALUES.to_vec();
        let keys_to_delete = keys[2..].to_vec();

        insert_test_data(&tx, &keys, &values).await;
        delete_keys(&tx, &keys_to_delete).await;

        let received_values = get_values_for_keys(&tx, keys.clone()).await;
        let mut expected_values: Vec<io::Result<Option<String>>> = values[..2]
            .into_iter()
            .map(|v| Ok(Some(v.to_string())))
            .collect();
        for _ in 0..keys_to_delete.len() {
            expected_values.push(Ok(None));
        }

        for (got, expected) in received_values.into_iter().zip(expected_values) {
            assert_eq!(got.unwrap(), expected.unwrap());
        }

        _store.close().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn set_and_clear() {
        let (tx, rv) = mpsc::channel(1);
        let _store = Store::new(rv, 2, STORE_PATH);

        let keys = KEYS.to_vec();
        let values = VALUES.to_vec();

        insert_test_data(&tx, &keys, &values).await;
        clear_test_data(&tx).await;

        let received_values = get_values_for_keys(&tx, keys.clone()).await;
        let expected_values: Vec<io::Result<Option<String>>> =
            keys.into_iter().map(|_| Ok(None)).collect();

        for (got, expected) in received_values.into_iter().zip(expected_values) {
            assert_eq!(got.unwrap(), expected.unwrap());
        }

        _store.close().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn persist_to_file() {
        let (tx, rv) = mpsc::channel(1);
        let _store = Store::new(rv, 2, STORE_PATH);

        let keys = KEYS.to_vec();
        let values = VALUES.to_vec();

        insert_test_data(&tx, &keys, &values).await;
        // Close the store

        _store.close().await;

        // Open new store instance
        let (tx, rv) = mpsc::channel(1);
        let _store = Store::new(rv, 2, STORE_PATH);

        let received_values = get_values_for_keys(&tx, keys.clone()).await;
        let expected_values: Vec<io::Result<Option<String>>> = values
            .into_iter()
            .map(|v| Ok(Some(v.to_string())))
            .collect();

        for (got, expected) in received_values.into_iter().zip(expected_values) {
            assert_eq!(got.unwrap(), expected.unwrap());
        }

        _store.close().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn persist_to_file_after_delete() {
        let (tx, rv) = mpsc::channel(1);
        let _store = Store::new(rv, 2, STORE_PATH);

        let keys = KEYS.to_vec();
        let values = VALUES.to_vec();
        let keys_to_delete = keys[2..].to_vec();

        insert_test_data(&tx, &keys, &values).await;
        delete_keys(&tx, &keys_to_delete).await;

        // Close the store

        _store.close().await;

        // Open new store instance
        let (tx, rv) = mpsc::channel(1);
        let _store = Store::new(rv, 2, STORE_PATH);

        let received_values = get_values_for_keys(&tx, keys.clone()).await;
        let mut expected_values: Vec<io::Result<Option<String>>> = values[..2]
            .into_iter()
            .map(|v| Ok(Some(v.to_string())))
            .collect();
        for _ in 0..keys_to_delete.len() {
            expected_values.push(Ok(None));
        }

        for (got, expected) in received_values.into_iter().zip(expected_values) {
            assert_eq!(got.unwrap(), expected.unwrap());
        }

        _store.close().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn persist_to_file_after_clear() {
        let (tx, rv) = mpsc::channel(1);
        let _store = Store::new(rv, 2, STORE_PATH);

        let keys = KEYS.to_vec();
        let values = VALUES.to_vec();

        insert_test_data(&tx, &keys, &values).await;
        clear_test_data(&tx).await;

        // Close the store

        _store.close().await;

        // Open new store instance
        let (tx, rv) = mpsc::channel(1);
        let _store = Store::new(rv, 2, STORE_PATH);

        let received_values = get_values_for_keys(&tx, keys.clone()).await;
        let expected_values: Vec<io::Result<Option<String>>> =
            keys.into_iter().map(|_| Ok(None)).collect();

        for (got, expected) in received_values.into_iter().zip(expected_values) {
            assert_eq!(got.unwrap(), expected.unwrap());
        }

        _store.close().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn close_aborts_tasks() {
        let (_, rv) = mpsc::channel(1);
        let _store = Store::new(rv, 2, STORE_PATH);

        for handler in &_store.handlers {
            assert!(!handler.is_finished())
        }

        // Close the store
        _store.close().await;

        for handler in &_store.handlers {
            assert!(handler.is_finished())
        }
    }

    async fn clear_test_data(tx: &Sender<Action>) {
        let (resp, recv) = oneshot::channel();
        let _ = tx.send(Action::Clear { resp }).await;
        let _ = recv.await.unwrap();
    }

    async fn delete_keys(tx: &Sender<Action>, keys_to_delete: &Vec<&str>) {
        for k in keys_to_delete {
            let key = k.to_string();
            let (resp, recv) = oneshot::channel();
            let _ = tx.send(Action::Del { key, resp }).await;
            let _ = recv.await.unwrap();
        }
    }

    async fn get_values_for_keys(
        tx: &Sender<Action>,
        keys: Vec<&str>,
    ) -> Vec<io::Result<Option<String>>> {
        let mut received_values = Vec::with_capacity(keys.len());

        for k in keys {
            let key = k.to_string();
            let (resp, recv) = oneshot::channel();
            let _ = tx.send(Action::Get { key, resp }).await;
            let _ = &received_values.push(recv.await.unwrap());
        }

        received_values
    }

    async fn insert_test_data(tx: &Sender<Action>, keys: &Vec<&str>, values: &Vec<&str>) {
        for (k, v) in keys.clone().into_iter().zip(values.clone()) {
            let key = k.to_string();
            let value = v.to_string();
            let (resp, recv) = oneshot::channel();
            let _ = tx.send(Action::Set { value, key, resp }).await;
            let _ = recv.await;
        }
    }
}
