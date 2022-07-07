use yalskv::util;
use yalskv::{kv, Store};

use std::time::SystemTime;

fn main() -> kv::Result<()> {
    std::fs::create_dir_all("target/db")?;
    let mut store = Store::open("target/db")?;

    let key = b"https://www.lipsum.com/feed/html";
    let val = b"[Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velit...]";

    store.insert(key, val)?;
    println!(
        "{:?}",
        &store
            .lookup(key)?
            .as_ref()
            .map(|bytes| String::from_utf8_lossy(bytes))
    );
    store.remove(key)?;
    println!("{:?}", store.lookup(key)?);

    const N: usize = 1000000;
    let now = SystemTime::now();

    let data = util::data(N, 42);
    for (key, val) in data.iter() {
        store.insert(key, val)?;
    }

    let data = util::shuffle(data, 142);
    for (key, _) in data.iter() {
        store.remove(key)?;
    }

    let ms = now.elapsed().unwrap().as_millis() as usize;
    let op = N * 2 * 1000 / ms;
    let kb = N * 1000 * (64 * 3) / ms / 1024;
    println!("n={} ms={} op={} kb={}", N, ms, op, kb);

    let limit = 4 * 1024 * 1024;
    store.fold(limit)?;

    Ok(())
}
