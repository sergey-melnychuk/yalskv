use yalskv::{kv, Store};

use std::time::SystemTime;

struct StringStore {
    store: Store,
}

impl StringStore {
    fn new(store: Store) -> Self {
        Self { store }
    }

    fn insert(&mut self, key: &str, val: &str) -> kv::Result<()> {
        self.store.insert(key.as_bytes(), val.as_bytes())
    }

    fn remove(&mut self, key: &str) -> kv::Result<()> {
        self.store.remove(key.as_bytes())
    }

    fn lookup(&mut self, key: &str) -> kv::Result<Option<String>> {
        let opt = self.store.lookup(key.as_bytes())?;
        Ok(opt.map(|slice| String::from_utf8_lossy(&slice).to_string()))
    }
}

fn main() -> kv::Result<()> {
    std::fs::create_dir_all("target/db")?;

    let mut store = StringStore::new(Store::open("target/db")?);

    let key = "https://www.lipsum.com/feed/html";
    let val = "Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velit...";

    store.insert(key, val)?;
    println!("{:?}", store.lookup(key)?);
    store.remove(key)?;
    println!("{:?}", store.lookup(key)?);

    const N: u64 = 1000000;
    let now = SystemTime::now();
    for _ in 0..N {
        store.insert(key, val)?;
        store.remove(key)?;
    }

    let ms = now.elapsed().unwrap().as_millis() as u64;
    let op = N * 2 * 1000 / ms;
    let kb = N * 1000 * (key.len() * 2 + val.len()) as u64 / ms / 1024;
    println!(
        "n={} ms={} op={} kb={} [k={} v={}]",
        N,
        ms,
        op,
        kb,
        key.len(),
        val.len()
    );

    Ok(())
}
