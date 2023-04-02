use std::fs::OpenOptions;
use std::io::{self, Write};
use std::io::{Seek, SeekFrom};
use std::os::unix::prelude::FileExt;
use std::path::{Path, PathBuf};
use std::{collections::BTreeMap, fs::File};

pub mod util;

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

    pub fn remove(&mut self, key: &[u8]) -> kv::Result<bool> {
        self.files.get_mut(&self.id).unwrap().remove(key)?;
        Ok(self.index.remove(key).is_some())
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

    fn id_to_dir_path(&self, id: &FileId) -> impl AsRef<Path> {
        self.id_to_path(id, "")
    }

    fn id_to_dat_path(&self, id: &FileId) -> impl AsRef<Path> {
        self.id_to_path(id, ".dat")
    }

    fn id_to_path(&self, id: &FileId, extension: &str) -> impl AsRef<Path> {
        let name = format!("{:020}{}", id.0, extension);
        let mut path = self.base.clone();
        path.push(&name);
        path
    }

    fn id_to_file(&self, id: &FileId) -> kv::Result<StoreFile> {
        let file = StoreFile::open(*id, self.id_to_path(id, ".dat"))?;
        Ok(file)
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn reduce(&mut self, limit: usize) -> kv::Result<()> {
        let path = self.id_to_dat_path(&self.id);
        let file = self.files.get_mut(&self.id).unwrap();

        let mut chunks = split(file, &self.base, limit)?;
        *file = StoreFile::make(self.id, &path)?;
        self.index = merge(file, &mut chunks)?;

        let path = self.id_to_dir_path(&self.id);
        std::fs::remove_dir_all(&path)?;
        Ok(())
    }

    pub fn file(&mut self) -> &mut StoreFile {
        self.files.get_mut(&self.id).unwrap()
    }
}

pub struct StoreFile {
    id: FileId,
    file: File,
    offset: u64,
    recent_peek: Option<Record>,
}

const INSERT: u64 = 1;
const REMOVE: u64 = 2;

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct FileId(u64);

#[derive(Debug, Clone)]
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

    pub fn val(&self) -> Option<&[u8]> {
        match self {
            Record::Insert(_, val) => Some(val),
            Record::Remove(_) => None,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Record::Insert(key, val) => 
                std::mem::size_of::<u64>()
                + 2 * std::mem::size_of::<u32>() 
                + key.len() + val.len(),
            Record::Remove(key) => 
                std::mem::size_of::<u64>() 
                + std::mem::size_of::<u32>() 
                + key.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl StoreFile {
    fn create(id: FileId, path: impl AsRef<Path>, truncate: bool) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(truncate)
            .write(true)
            .read(true)
            .open(&path)?;
        let offset = file.metadata()?.len() as u64;
        Ok(Self {
            id,
            file,
            offset,
            recent_peek: None,
        })
    }

    fn open(id: FileId, path: impl AsRef<Path>) -> io::Result<Self> {
        Self::create(id, path, false)
    }

    fn make(id: FileId, path: impl AsRef<Path>) -> io::Result<Self> {
        Self::create(id, path, true)
    }

    fn insert(&mut self, key: &[u8], val: &[u8]) -> io::Result<IndexEntry> {
        let key_len = key.len() as u32;
        let val_len = val.len() as u32;
        //self.file.seek(SeekFrom::Start(self.offset))?;
        self.file.write_all(&INSERT.to_be_bytes())?;
        self.file.write_all(&key_len.to_be_bytes())?;
        self.file.write_all(&val_len.to_be_bytes())?;
        self.file.write_all(key)?;
        self.file.write_all(val)?;
        self.file.flush()?;

        let length = std::mem::size_of::<u64>() as u64 
            + 2 * std::mem::size_of::<u32>() as u64 
            + key_len as u64 + val_len as u64;
        self.offset += length;

        Ok(IndexEntry {
            file: self.id,
            offset: self.offset - val_len as u64,
            length: val_len as u64,
        })
    }

    fn remove(&mut self, key: &[u8]) -> io::Result<()> {
        let key_len = key.len() as u32;
        //self.file.seek(SeekFrom::Start(self.offset))?;
        self.file.write_all(&REMOVE.to_be_bytes())?;
        self.file.write_all(&key_len.to_be_bytes())?;
        self.file.write_all(key)?;
        self.file.flush()?;

        let length = std::mem::size_of::<u64>() as u64 + std::mem::size_of::<u32>() as u64 + key_len as u64;
        self.offset += length;

        Ok(())
    }

    fn exec(&mut self, record: &Record) -> io::Result<()> {
        match record {
            Record::Insert(key, val) => {
                self.insert(key, val)?;
                Ok(())
            }
            Record::Remove(key) => {
                self.remove(key)?;
                Ok(())
            }
        }
    }

    fn read(&mut self, offset: u64, buffer: &mut [u8]) -> io::Result<()> {
        let pos = self.file.stream_position()?;
        self.file.read_exact_at(buffer, offset)?;
        self.file.seek(SeekFrom::Start(pos))?;
        Ok(())
    }

    pub fn read_record(&mut self) -> io::Result<Record> {
        if let Some(record) = self.recent_peek.take() {
            self.offset += record.len() as u64;
            return Ok(record);
        }
        let mut buf = [0u8; 8];
        self.file.read_exact_at(&mut buf[..], self.offset)?;
        let op = u64::from_be_bytes(buf);

        self.file.read_exact_at(&mut buf[0..4], self.offset + 8)?;
        let key_len = u32::from_be_bytes(buf[0..4].try_into().unwrap());

        // TODO Add sanity check for max key/value length
        match op {
            INSERT => {
                self.file.read_exact_at(&mut buf[4..8], self.offset + 12)?;
                let val_len = u32::from_be_bytes(buf[4..8].try_into().unwrap());

                let mut buf = vec![0u8; (key_len + val_len) as usize];
                self.file.read_exact_at(&mut buf[..], self.offset + 16)?;
                self.offset += 16 + key_len as u64 + val_len as u64;

                let val = buf.split_off(key_len as usize);
                Ok(Record::Insert(buf, val))
            }
            REMOVE => {
                let mut buf = vec![0u8; key_len as usize];
                self.file.read_exact_at(&mut buf[..], self.offset + 12)?;
                self.offset += 12 + key_len as u64;
                Ok(Record::Remove(buf))
            }
            _ => Err(std::io::Error::from(std::io::ErrorKind::Unsupported)),
        }
    }

    pub fn peek_record(&mut self) -> io::Result<&Record> {
        if self.recent_peek.is_some() {
            return Ok(self.recent_peek.as_ref().unwrap());
        }
        let record = self.read_record()?;
        self.offset -= record.len() as u64;
        self.recent_peek = Some(record);
        Ok(self.recent_peek.as_ref().unwrap())
    }

    pub fn reset(&mut self) -> io::Result<()> {
        self.file.seek(SeekFrom::Start(0))?;
        self.offset = 0;
        Ok(())
    }

    pub fn unset(&mut self) -> io::Result<()> {
        self.offset = self.file.metadata()?.len();
        self.file.seek(SeekFrom::End(0))?;
        Ok(())
    }
}

impl Iterator for StoreFile {
    type Item = Record;
    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        self.read_record().ok()
    }
}

fn split(
    src: &mut StoreFile,
    base: impl AsRef<Path>,
    split_size_bytes: usize,
) -> io::Result<Vec<StoreFile>> {
    let dir = format!("{:020}", src.id.0);
    let mut path: PathBuf = base.as_ref().to_path_buf();
    path.push(dir);
    std::fs::create_dir_all(&path)?;

    let mut result = Vec::new();
    let mut records = Vec::new();
    let mut idx = 0;
    let mut len = 0;

    fn make_file(id: FileId, base: impl AsRef<Path>) -> io::Result<StoreFile> {
        let name = format!("{:020}.dat", id.0);
        let mut path: PathBuf = base.as_ref().to_path_buf();
        path.push(name);
        StoreFile::open(id, path)
    }

    fn dump_file(file: &mut StoreFile, mut records: Vec<Record>) -> io::Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        records.sort_by(|a, b| a.key().cmp(b.key()));
        for record in records {
            file.exec(&record)?;
        }
        file.file.flush()?;
        Ok(())
    }

    src.reset()?;
    while let Ok(record) = src.read_record() {
        if len + record.len() > split_size_bytes {
            let mut file = make_file(FileId(idx), &path)?;
            dump_file(&mut file, records)?;
            result.push(file);
            records = Vec::new();
            len = 0;
            idx += 1;
        }

        len += record.len();
        records.push(record);
    }

    let mut file = make_file(FileId(idx), &path)?;
    dump_file(&mut file, records)?;
    result.push(file);

    for src in result.iter_mut() {
        src.file.flush()?;
        src.reset()?;
    }

    Ok(result)
}

fn merge(dst: &mut StoreFile, srcs: &mut [StoreFile]) -> io::Result<BTreeMap<Vec<u8>, IndexEntry>> {
    fn pick(srcs: &'_ mut [StoreFile]) -> Option<&'_ mut StoreFile> {
        srcs.iter_mut()
            .flat_map(|src| src.peek_record().ok().cloned().map(|rec| (rec, src)))
            .min_by(|(a, _), (b, _)| a.key().cmp(b.key()))
            .map(|(_, src)| src)
    }

    let mut index = BTreeMap::new();
    let mut current_key: Option<Vec<u8>> = None;
    let mut current_val: Option<Vec<u8>> = None;
    while let Some(src) = pick(srcs) {
        let record = src.read_record()?;
        if current_key.is_none() {
            current_key = Some(record.key().to_vec());
        }
        if record.key() != current_key.as_ref().unwrap() {
            if current_val.is_some() {
                let key = current_key.as_ref().unwrap();
                let val = current_val.as_ref().unwrap();
                let entry = dst.insert(key, val)?;
                index.insert(current_key.as_ref().unwrap().to_vec(), entry);
            }
            current_key = Some(record.key().to_vec());
        }
        current_val = record.val().map(|slice| slice.to_vec());
    }

    if current_val.is_some() {
        let entry = dst.insert(current_key.as_ref().unwrap(), current_val.as_ref().unwrap())?;
        index.insert(current_key.as_ref().unwrap().to_vec(), entry);
    }

    dst.file.flush()?;
    Ok(index)
}
