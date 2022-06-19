use std::fs::OpenOptions;
use std::io::{self, Write};
use std::os::unix::prelude::FileExt;
use std::path::{Path, PathBuf};
use std::{collections::HashMap, fs::File};

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
    files: HashMap<FileId, StoreFile>,
    index: HashMap<Vec<u8>, IndexEntry>,
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
            files: HashMap::default(),
            index: Default::default(),
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
        let name = format!("{:09}.dat", id.0);
        let mut path = self.base.clone();
        path.push(&name);
        path
    }

    fn id_to_file(&self, id: &FileId) -> kv::Result<StoreFile> {
        let file = StoreFile::make(*id, self.id_to_path(id))?;
        Ok(file)
    }

    #[allow(dead_code)]
    fn len(&self) -> usize {
        self.index.len()
    }
}

struct StoreFile {
    id: FileId,
    file: File,
    offset: u64,
}

const INSERT: u64 = 1;
const REMOVE: u64 = 2;

#[derive(Copy, Clone, Eq, PartialEq, Hash)]
struct FileId(u64);

impl StoreFile {
    fn make(id: FileId, path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .read(true)
            .open(&path)?;
        Ok(Self {
            id,
            file,
            offset: 0,
        })
    }

    #[allow(dead_code)]
    fn open(id: FileId, path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(false)
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
        self.file.write_all(&REMOVE.to_be_bytes())?;
        self.file.write_all(&key_len.to_be_bytes())?;
        self.file.write_all(key)?;
        self.file.flush()?;

        let length = std::mem::size_of::<u64>() as u64 * 2 + key_len;
        self.offset += length;

        Ok(())
    }

    fn read(&mut self, offset: u64, buffer: &mut [u8]) -> io::Result<()> {
        self.file.read_exact_at(buffer, offset)
    }
}
