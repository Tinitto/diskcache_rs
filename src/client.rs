use crate::store::{Action, Store};
use core::option::Option;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

pub struct Client {
    action_sender: mpsc::Sender<Action>,
    _store: Option<Store>,
}

impl Client {
    pub fn new(store_path: &str, num_of_workers: usize) -> Client {
        let (action_sender, action_receiver) = mpsc::channel(10);
        let store = Store::new(action_receiver, num_of_workers, store_path);
        Client {
            action_sender,
            _store: Some(store),
        }
    }

    pub async fn set(&mut self, key: String, value: String) {
        let (tx, rv) = oneshot::channel();
        let _ = self
            .action_sender
            .send(Action::Set {
                key,
                value,
                resp: tx,
            })
            .await;

        let _ = rv.await;
    }

    pub async fn get(&self, key: &str) -> Option<String> {
        let (tx, rv) = oneshot::channel();
        let _ = self
            .action_sender
            .send(Action::Get {
                key: key.to_string(),
                resp: tx,
            })
            .await;
        rv.await.unwrap()
    }

    pub async fn delete(&mut self, key: &str) -> Option<String> {
        let (tx, rv) = oneshot::channel();
        let _ = self
            .action_sender
            .send(Action::Del {
                key: key.to_string(),
                resp: tx,
            })
            .await;

        rv.await.unwrap()
    }

    pub async fn clear(&mut self) {
        let (tx, rv) = oneshot::channel();
        let _ = self.action_sender.send(Action::Clear { resp: tx }).await;
        let _ = rv.await;
    }

    pub async fn close(&mut self) {
        let _ = self.action_sender.send(Action::Close).await;
        let store = self._store.take().unwrap();
        store.await;
    }
}
