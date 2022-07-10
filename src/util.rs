use rand::prelude::SliceRandom;
use rand::prelude::StdRng;
use rand::{RngCore, SeedableRng};

pub fn data(count: usize, seed: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..count)
        .into_iter()
        .map(|_| {
            let mut key = Vec::with_capacity(64);
            let mut val = Vec::with_capacity(64);
            for _ in 0..8 {
                key.extend_from_slice(&rng.next_u64().to_be_bytes());
                val.extend_from_slice(&rng.next_u64().to_be_bytes());
            }
            (key, val)
        })
        .collect()
}

pub fn mix<T>(mut data: Vec<T>, seed: u64) -> Vec<T> {
    let mut rng = StdRng::seed_from_u64(seed);
    data.shuffle(&mut rng);
    data
}

pub fn hex(src: &[u8]) -> String {
    src.iter()
        .map(|x| format!("{:02x}", x))
        .collect::<Vec<_>>()
        .concat()
}
