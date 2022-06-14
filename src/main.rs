use diskcache_rs::Client;

#[tokio::main]
async fn main() {
    let mut store = Client::new("db", 4);
    let keys = ["hey", "hi", "yoo-hoo", "bonjour"].to_vec();
    let values = ["English", "English", "Slang", "French"].to_vec();

    // Setting the values
    println!("[Inserting key-value pairs]");
    for (k, v) in keys.clone().into_iter().zip(values) {
        store.set(k.to_string(), v.to_string()).await;
    }

    // Getting the values
    println!("[After insert]");
    for k in keys.clone() {
        let got = store.get(k).await.unwrap();
        println!("For key: {:?}, Got: {:?}", k, got);
    }

    // Deleting some values
    for k in &keys[2..] {
        let removed = store.delete(*k).await;
        println!("Removed: key: {:?}, resp: {:?}", k, removed);
    }

    for k in &keys {
        let got = store.get(*k).await;
        println!("[After delete: For key: {:?}, Got: {:?}", k, got);
    }

    // Deleting all values
    let cleared = store.clear().await;
    println!("Cleared: {:?}", cleared);

    println!("[After clear]");
    for k in &keys {
        let got = store.get(*k).await;
        println!("For key: {:?}, Got: {:?}", k, got);
    }
    store.close().await;
}
