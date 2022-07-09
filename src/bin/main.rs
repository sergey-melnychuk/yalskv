use yalskv::util::{self, hex};
use yalskv::{kv, Record, Store};

use std::time::SystemTime;

fn main() -> kv::Result<()> {
    std::fs::create_dir_all("target/db")?;
    let mut store = Store::open("target/db")?;

    const N: usize = 1000000;
    let now = SystemTime::now();

    let data = util::data(N, 42);
    for (key, val) in data.iter() {
        store.insert(key, val)?;
    }

    let ms = (now.elapsed().unwrap().as_millis() as usize).max(1);
    let op = N * 1000 / ms;
    let kb = N * 1000 * (64 + 64) / ms / 1024;
    println!("n={} ms={} op={} kb={}", N, ms, op, kb);
    println!("insert: ok");

    let limit = 1024 * 1024 * 32;
    store.reduce(limit)?;
    println!("reduce: ok");

    let data = util::shuffle(data, 1);
    for (key, value) in data.iter() {
        if let Some(stored) = store.lookup(key)? {
            if &stored != value {
                eprintln!("!match: key={}", hex(key));
            }
        } else {
            eprintln!("!found: key={}", hex(key));
        }
    }
    println!("lookup: ok");

    store.file().reset()?;
    let mut prev: Option<Record> = None;
    for (i, next) in store.file().enumerate() {
        if prev.is_none() {
            prev = Some(next);
            continue;
        }

        if prev.as_ref().unwrap().key() > next.key() {
            println!(
                "!sorted (i={}):\n\tprev={}\n\tnext={}",
                i,
                hex(prev.as_ref().unwrap().key()),
                hex(next.key())
            );
        }
        prev = Some(next);
    }
    println!("sorted: ok");

    store.file().unset()?;
    let data = util::shuffle(data, 2);
    for (key, _) in data.iter() {
        if !store.remove(key)? {
            eprintln!("!exist: key={}", hex(key));
        }
    }
    store.reduce(limit)?;

    store.file().reset()?;
    let count = store.file().count();
    if count > 0 {
        eprintln!("!empty: {}", count);
    }
    println!("remove: ok");

    Ok(())
}
