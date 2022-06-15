use crate::store::{Action, Store};
use core::option::Option;
use std::io::{Error, ErrorKind};
use tokio::io;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

pub struct Client {
    action_sender: mpsc::Sender<Action>,
    store: Option<Store>,
}

impl Client {
    pub fn new(store_path: &str, num_of_workers: usize) -> Client {
        let (action_sender, action_receiver) = mpsc::channel(10);
        Client {
            action_sender,
            store: Some(Store::new(action_receiver, num_of_workers, store_path)),
        }
    }

    pub async fn set(&mut self, key: String, value: String) -> io::Result<Option<String>> {
        let (tx, rv) = oneshot::channel();
        let action = Action::Set {
            key,
            value,
            resp: tx,
        };
        self.send_single_record_action(action, rv).await
    }

    pub async fn get(&mut self, key: &str) -> io::Result<Option<String>> {
        let (tx, rv) = oneshot::channel();
        let action = Action::Get {
            key: key.to_string(),
            resp: tx,
        };

        self.send_single_record_action(action, rv).await
    }

    pub async fn delete(&mut self, key: &str) -> io::Result<Option<String>> {
        let (tx, rv) = oneshot::channel();
        let action = Action::Del {
            key: key.to_string(),
            resp: tx,
        };
        self.send_single_record_action(action, rv).await
    }

    pub async fn clear(&mut self) -> io::Result<()> {
        let (tx, rv) = oneshot::channel();
        let action = Action::Clear { resp: tx };
        self.send_single_record_action(action, rv).await
    }

    async fn send_single_record_action<T>(
        &mut self,
        action: Action,
        rv: oneshot::Receiver<io::Result<T>>,
    ) -> io::Result<T> {
        if let Err(e) = self.action_sender.send(action).await {
            return Err(Error::new(ErrorKind::ConnectionRefused, e.to_string()));
        }

        match rv.await {
            Ok(v) => v,
            Err(e) => Err(Error::new(ErrorKind::ConnectionRefused, e.to_string())),
        }
    }

    pub async fn close(&mut self) {
        let store = self.store.take().unwrap();
        store.close().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    const STORE_PATH: &str = "client_db";
    const KEYS: [&str; 4] = ["hey", "hi", "yoo-hoo", "bonjour"];
    const VALUES: [&str; 4] = ["English", "English", "Slang", "French"];

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn set_and_read_multiple_key_value_pairs() {
        let mut client = Client::new(STORE_PATH, 2);

        let keys = KEYS.to_vec();
        let values = VALUES.to_vec();

        insert_test_data(&mut client, &keys, &values).await;
        let received_values = get_values_for_keys(&mut client, keys).await;

        let expected_values: Vec<io::Result<Option<String>>> = values
            .into_iter()
            .map(|v| Ok(Some(v.to_string())))
            .collect();

        for (got, expected) in received_values.into_iter().zip(expected_values) {
            assert_eq!(got.unwrap(), expected.unwrap());
        }

        client.close().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn set_and_delete_multiple_key_value_pairs() {
        let mut client = Client::new(STORE_PATH, 2);
        let keys = KEYS.to_vec();
        let values = VALUES.to_vec();
        let keys_to_delete = keys[2..].to_vec();

        insert_test_data(&mut client, &keys, &values).await;

        for k in &keys_to_delete {
            let _ = &client.delete(*k).await;
        }

        let received_values = get_values_for_keys(&mut client, keys.clone()).await;
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

        client.close().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn set_and_clear() {
        let mut client = Client::new(STORE_PATH, 2);

        let keys = KEYS.to_vec();
        let values = VALUES.to_vec();

        insert_test_data(&mut client, &keys, &values).await;
        let _ = client.clear().await;

        let received_values = get_values_for_keys(&mut client, keys.clone()).await;
        let expected_values: Vec<io::Result<Option<String>>> =
            keys.into_iter().map(|_| Ok(None)).collect();

        for (got, expected) in received_values.into_iter().zip(expected_values) {
            assert_eq!(got.unwrap(), expected.unwrap());
        }

        client.close().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn persist_to_file() {
        let mut client = Client::new(STORE_PATH, 2);

        let keys = KEYS.to_vec();
        let values = VALUES.to_vec();

        insert_test_data(&mut client, &keys, &values).await;
        // close old client and store instances
        client.close().await;

        // Open new store instance
        let mut client = Client::new(STORE_PATH, 2);

        let received_values = get_values_for_keys(&mut client, keys.clone()).await;
        let expected_values: Vec<io::Result<Option<String>>> = values
            .into_iter()
            .map(|v| Ok(Some(v.to_string())))
            .collect();

        for (got, expected) in received_values.into_iter().zip(expected_values) {
            assert_eq!(got.unwrap(), expected.unwrap());
        }

        client.close().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn persist_to_file_after_delete() {
        let mut client = Client::new(STORE_PATH, 2);

        let keys = KEYS.to_vec();
        let values = VALUES.to_vec();
        let keys_to_delete = keys[2..].to_vec();

        insert_test_data(&mut client, &keys, &values).await;
        delete_keys(&mut client, &keys_to_delete).await;

        // Close the store
        client.close().await;

        // Open new store instance
        let mut client = Client::new(STORE_PATH, 2);

        let received_values = get_values_for_keys(&mut client, keys.clone()).await;
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

        client.close().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn persist_to_file_after_clear() {
        let mut client = Client::new(STORE_PATH, 2);

        let keys = KEYS.to_vec();
        let values = VALUES.to_vec();

        insert_test_data(&mut client, &keys, &values).await;
        let _ = client.clear().await;

        // Close the store
        client.close().await;

        // Open new store instance
        let mut client = Client::new(STORE_PATH, 2);

        let received_values = get_values_for_keys(&mut client, keys.clone()).await;
        let expected_values: Vec<io::Result<Option<String>>> =
            keys.into_iter().map(|_| Ok(None)).collect();

        for (got, expected) in received_values.into_iter().zip(expected_values) {
            assert_eq!(got.unwrap(), expected.unwrap());
        }

        client.close().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn close_must_close_store() {
        let mut client = Client::new(STORE_PATH, 2);
        let _ = client.set(KEYS[0].to_string(), VALUES[0].to_string()).await;

        assert!(client.get(KEYS[0]).await.is_ok());

        client.close().await;

        assert!(client.get(KEYS[0]).await.is_err());
    }

    async fn delete_keys(client: &mut Client, keys_to_delete: &Vec<&str>) {
        for k in keys_to_delete {
            let _ = &client.delete(*k).await;
        }
    }

    async fn get_values_for_keys(
        client: &mut Client,
        keys: Vec<&str>,
    ) -> Vec<io::Result<Option<String>>> {
        let mut received_values = Vec::with_capacity(keys.len());

        for k in keys {
            let _ = &received_values.push(client.get(k).await);
        }

        received_values
    }

    async fn insert_test_data(client: &mut Client, keys: &Vec<&str>, values: &Vec<&str>) {
        for (k, v) in keys.clone().into_iter().zip(values) {
            let _ = &client.set(k.to_string(), v.to_string()).await;
        }
    }
}
