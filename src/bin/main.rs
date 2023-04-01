use yalskv::util::{data, hex, mix};
use yalskv::{kv, Store};

use std::time::SystemTime;

fn main() -> kv::Result<()> {
    std::fs::create_dir_all("target/db")?;
    let mut store = Store::open("target/db")?;

    const N: usize = 1000000;
    let limit = 1024 * 1024 * 32;
    let data = data(N, 42);
    println!("N={N} limit={limit}");

    let mut now = SystemTime::now();
    for (key, val) in data.iter() {
        store.insert(key, val)?;
    }

    let ms = (now.elapsed().unwrap().as_millis() as usize).max(1);
    let op = N * 1000 / ms;
    let kb = N * 1000 * (64 + 64 + 3 * 8) / ms / 1024;
    println!("insert: ok (ms={ms} op={op} kb={kb})");

    now = SystemTime::now();
    store.reduce(limit)?;
    let ms = (now.elapsed().unwrap().as_millis() as usize).max(1);
    let op = N * 1000 / ms;
    let kb = N * 1000 * (64 + 64 + 3 * 8) / ms / 1024;
    println!("reduce: ok (ms={ms} op={op} kb={kb})");

    let data = mix(data, 1);
    now = SystemTime::now();
    let mut found = Vec::with_capacity(data.len());
    for (key, _) in data.iter() {        
        let val = store.lookup(key)?.unwrap_or_default();
        found.push(val);
    }
    let ms = (now.elapsed().unwrap().as_millis() as usize).max(1);
    let op = N * 1000 / ms;
    let kb = N * 1000 * (64 + 64 + 3 * 8) / ms / 1024;
    println!("lookup: ok (ms={ms} op={op} kb={kb})");
    for ((key, val), res) in data.iter().zip(found.iter()) {        
        if res.is_empty() {
            eprintln!("!found: key={}", hex(key));
        } else if val != res {
            eprintln!("!match: key={}", hex(key));
        }
    }

    now = SystemTime::now();
    store.file().reset()?;
    let mut found = Vec::with_capacity(data.len());
    for rec in store.file() {
        found.push(rec.key().to_vec());
    }
    let ms = (now.elapsed().unwrap().as_millis() as usize).max(1);
    let op = N * 1000 / ms;
    let kb = N * 1000 * (64 + 64 + 3 * 8) / ms / 1024;
    println!("sorted: ok (ms={ms} op={op} kb={kb})");

    let mut prev: Vec<u8> = Default::default();
    for (i, next) in found.into_iter().enumerate() {
        if prev.is_empty() {
            prev = next;
            continue;
        }

        if prev > next {
            println!(
                "!sorted (i={}):\n\tprev={}\n\tnext={}",
                i,
                hex(&prev),
                hex(&next)
            );
        }
        prev = next;
    }

    let data = mix(data, 2);
    now = SystemTime::now();
    store.file().unset()?;
    for (key, _) in data.iter() {
        if !store.remove(key)? {
            eprintln!("!exist: key={}", hex(key));
        }
    }
    let ms = (now.elapsed().unwrap().as_millis() as usize).max(1);
    let op = N * 1000 / ms;
    let kb = N * 1000 * (64 + 64 + 3 * 8) / ms / 1024;
    println!("remove: ok (ms={ms} op={op} kb={kb})");

    now = SystemTime::now();
    store.reduce(limit)?;
    let ms = (now.elapsed().unwrap().as_millis() as usize).max(1);
    let op = N * 1000 / ms;
    let kb = N * 1000 * (64 + 64 + 3 * 8) / ms / 1024;
    println!("reduce: ok (ms={ms} op={op} kb={kb})");

    store.file().reset()?;
    let count = store.file().count();
    if count > 0 {
        eprintln!("!empty: {}", count);
    }
    Ok(())
}
