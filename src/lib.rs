use std::fs::OpenOptions;
use std::io::{self, Write};
use std::io::{Seek, SeekFrom};
use std::os::unix::prelude::FileExt;
use std::path::{Path, PathBuf};
use std::{collections::BTreeMap, fs::File};

pub mod kv {

    #[derive(Debug)]
    pub enum Error {
        IO(std::io::Error),
        Unknown(String),
    }

    impl From<std::io::Error> for Error {
        fn from(e: std::io::Error) -> Self {
            Self::IO(e)
        }
    }

    pub type Result<T> = std::result::Result<T, Error>;
}

struct IndexEntry {
    file: FileId,
    offset: u64,
    length: u64,
}

pub struct Store {
    id: FileId,
    base: PathBuf,
    files: BTreeMap<FileId, StoreFile>,
    index: BTreeMap<Vec<u8>, IndexEntry>,
}

impl Store {
    pub fn open(base: &str) -> kv::Result<Self> {
        // TODO:
        // 1. Scan base dir (report error if the dir is missing)
        // 2. Build index from data files
        // 3. Compact files (in background)
        // 4. Create a new file

        let id = FileId(1);
        let mut this = Self {
            id,
            base: PathBuf::from(base),
            files: BTreeMap::default(),
            index: BTreeMap::default(),
        };

        this.files.insert(id, this.id_to_file(&id)?);

        Ok(this)
    }

    pub fn insert(&mut self, key: &[u8], val: &[u8]) -> kv::Result<()> {
        let entry = self.files.get_mut(&self.id).unwrap().insert(key, val)?;
        self.index.insert(key.to_vec(), entry);
        Ok(())
    }

    pub fn remove(&mut self, key: &[u8]) -> kv::Result<()> {
        self.files.get_mut(&self.id).unwrap().remove(key)?;
        self.index.remove(key);
        Ok(())
    }

    pub fn lookup(&mut self, key: &[u8]) -> kv::Result<Option<Vec<u8>>> {
        if let Some(IndexEntry {
            file,
            offset,
            length,
        }) = self.index.get(key)
        {
            if !self.files.contains_key(file) {
                self.files.insert(*file, self.id_to_file(file)?);
            }
            let mut buffer = vec![0u8; *length as usize];
            self.files
                .get_mut(file)
                .unwrap()
                .read(*offset, &mut buffer[..])?;
            return Ok(Some(buffer));
        }
        Ok(None)
    }

    fn id_to_path(&self, id: &FileId) -> PathBuf {
        let name = format!("{:020}.dat", id.0);
        let mut path = self.base.clone();
        path.push(&name);
        path
    }

    fn id_to_file(&self, id: &FileId) -> kv::Result<StoreFile> {
        let file = StoreFile::open(*id, self.id_to_path(id))?;
        Ok(file)
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub struct StoreFile {
    id: FileId,
    file: File,
    offset: u64,
}

const INSERT: u64 = 1;
const REMOVE: u64 = 2;

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct FileId(u64);

#[derive(Debug)]
pub enum Record {
    Insert(Vec<u8>, Vec<u8>),
    Remove(Vec<u8>),
}

impl Record {
    pub fn key(&self) -> &[u8] {
        match self {
            Record::Insert(key, _) => key,
            Record::Remove(key) => key,
        }
    }

    pub fn bytes(&self) -> usize {
        match self {
            Record::Insert(key, val) => 8 + 16 + key.len() + val.len(),
            Record::Remove(key) => 8 + 8 + key.len(),
        }
    }
}

impl StoreFile {
    fn open(id: FileId, path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .open(&path)?;
        let offset = file.metadata()?.len() as u64;
        Ok(Self { id, file, offset })
    }

    fn insert(&mut self, key: &[u8], val: &[u8]) -> io::Result<IndexEntry> {
        let key_len = key.len() as u64;
        let val_len = val.len() as u64;
        self.file.seek(SeekFrom::Start(self.offset))?;
        self.file.write_all(&INSERT.to_be_bytes())?;
        self.file.write_all(&key_len.to_be_bytes())?;
        self.file.write_all(&val_len.to_be_bytes())?;
        self.file.write_all(key)?;
        self.file.write_all(val)?;
        self.file.flush()?;

        self.offset += std::mem::size_of::<u64>() as u64 * 3 + key_len + val_len;
        let offset = self.offset - val_len;

        Ok(IndexEntry {
            file: self.id,
            offset,
            length: val_len,
        })
    }

    fn remove(&mut self, key: &[u8]) -> io::Result<()> {
        let key_len = key.len() as u64;
        self.file.seek(SeekFrom::Start(self.offset))?;
        self.file.write_all(&REMOVE.to_be_bytes())?;
        self.file.write_all(&key_len.to_be_bytes())?;
        self.file.write_all(key)?;
        self.file.flush()?;

        let length = std::mem::size_of::<u64>() as u64 * 2 + key_len;
        self.offset += length;

        Ok(())
    }

    fn exec(&mut self, record: &Record) -> io::Result<()> {
        match record {
            Record::Insert(key, val) => {
                self.insert(key, val)?;
                Ok(())
            }
            Record::Remove(key) => self.remove(key),
        }
    }

    fn read(&mut self, offset: u64, buffer: &mut [u8]) -> io::Result<()> {
        self.file.read_exact_at(buffer, offset)
    }

    pub fn read_record(&mut self) -> io::Result<Record> {
        let mut buf = [0u8; 8];
        self.file.read_exact_at(&mut buf[..], self.offset)?;
        let op = u64::from_be_bytes(buf);

        self.file.read_exact_at(&mut buf[..], self.offset + 8)?;
        let key_len = u64::from_be_bytes(buf);

        // TODO Add sanity check for max key/value length
        match op {
            INSERT => {
                self.file.read_exact_at(&mut buf[..], self.offset + 16)?;
                let val_len = u64::from_be_bytes(buf);

                let mut buf = vec![0u8; (key_len + val_len) as usize];
                self.file.read_exact_at(&mut buf[..], self.offset + 24)?;
                self.offset += 24 + key_len + val_len;

                let val = buf.split_off(key_len as usize);
                Ok(Record::Insert(buf, val))
            }
            REMOVE => {
                let mut buf = vec![0u8; key_len as usize];
                self.file.read_exact_at(&mut buf[..], self.offset + 16)?;
                self.offset += 16 + key_len;
                Ok(Record::Remove(buf))
            }
            _ => Err(std::io::Error::from(std::io::ErrorKind::Unsupported)),
        }
    }

    pub fn peek_record(&mut self) -> io::Result<Record> {
        let record = self.read_record()?;
        self.offset -= record.bytes() as u64;
        Ok(record)
    }

    fn reset(&mut self) -> io::Result<()> {
        self.file.seek(SeekFrom::Start(0))?;
        Ok(())
    }
}

impl Iterator for StoreFile {
    type Item = Record;
    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        self.read_record().ok()
    }
}

fn sort(
    idx: &BTreeMap<Vec<u8>, IndexEntry>,
    src: &mut StoreFile,
    dst: &mut StoreFile,
) -> io::Result<BTreeMap<Vec<u8>, IndexEntry>> {
    let mut updated: BTreeMap<Vec<u8>, IndexEntry> = BTreeMap::new();

    for (key, IndexEntry { offset, length, .. }) in idx.iter() {
        let mut val = vec![0u8; *length as usize];
        src.read(*offset, &mut val)?;
        let entry = dst.insert(key, &val)?;
        updated.insert(key.to_vec(), entry);
    }

    Ok(updated)
}

// Both input StoreFile are expected to have entries sorted by key
fn merge(dst: &mut StoreFile, one: &mut StoreFile, two: &mut StoreFile) -> io::Result<()> {
    fn pick<'a>(one: &'a mut StoreFile, two: &'a mut StoreFile) -> Option<&'a mut StoreFile> {
        match (one.peek_record().ok(), two.peek_record().ok()) {
            (None, None) => None,
            (Some(_), None) => Some(one),
            (None, Some(_)) => Some(two),
            (Some(a), Some(b)) if a.key() <= b.key() => Some(one),
            _ => Some(two),
        }
    }

    while let Some(next) = pick(one, two) {
        let record = next.read_record()?;
        dst.exec(&record)?;
    }

    Ok(())
}
