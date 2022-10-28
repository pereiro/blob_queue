use byteorder::{LittleEndian, ReadBytesExt};
use crc32fast::Hasher;
use std::fs::File;
use std::io;
use std::io::{ErrorKind, Read, Write};
use std::time::{SystemTime, UNIX_EPOCH};

const MAGIC: u32 = 0xDADADADA;
const VERSION: u32 = 0x00000000;
const RESERVED: [u32; 11] = [0; 11];

pub struct Container {
    file_header: FileHeader,
    data_header: DataHeader,
    toc: Vec<TocEntry>,
    data: Vec<u8>,
}

pub struct FileHeader {
    magic: u32,
    checksum: u32,
}

pub struct DataHeader {
    version: u32,
    type_id: u32,
    toc_size: u32,
    reserved: [u32; 11],
}

pub struct TocEntry {
    writer_id: u32,
    data_size: u32,
    timestamp: u64,
}

impl FileHeader {
    pub fn new(checksum: u32) -> Self {
        Self {
            magic: MAGIC,
            checksum,
        }
    }
    pub fn as_bytes(&self) -> Vec<u8> {
        as_u8_slice::<u32>(&[self.magic, self.checksum]).to_vec()
    }
}

impl DataHeader {
    pub fn new(version: u32, type_id: u32, toc_size: u32, reserved: [u32; 11]) -> Self {
        Self {
            version,
            type_id,
            toc_size,
            reserved,
        }
    }
    pub fn as_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(as_u8_slice::<u32>(&[
            self.version,
            self.type_id,
            self.toc_size,
        ]));
        buf.extend_from_slice(as_u8_slice::<u32>(&self.reserved));
        buf
    }
}

impl TocEntry {
    pub fn new(writer_id: u32, data_size: u32) -> Self {
        Self::new_with_timestamp(
            writer_id,
            data_size,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        )
    }
    pub fn new_with_timestamp(writer_id: u32, data_size: u32, timestamp: u64) -> Self {
        Self {
            writer_id,
            data_size,
            timestamp,
        }
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(as_u8_slice::<u32>(&[self.writer_id, self.data_size]));
        buf.extend_from_slice(as_u8_slice::<u64>(&[self.timestamp]));
        buf
    }
}

impl Container {
    pub fn new(type_id: u32) -> Self {
        Self {
            file_header: FileHeader::new(0),
            data_header: DataHeader::new(VERSION, type_id, 0, RESERVED),
            toc: Vec::new(),
            data: Vec::new(),
        }
    }

    pub fn push(&mut self, writer_id: u32, data: &[u8]) {
        self.data.write_all(data).unwrap();
        let toc_entry = TocEntry::new(writer_id, data.len() as u32);
        self.toc.push(toc_entry);
    }

    pub fn get_data_header(&self) -> DataHeader {
        DataHeader::new(
            VERSION,
            self.data_header.type_id,
            self.toc.len() as u32,
            RESERVED,
        )
    }

    pub fn checksum(&self) -> u32 {
        let mut hasher = Hasher::new();
        hasher.update(self.get_data_header().as_bytes().as_slice());
        self.toc
            .iter()
            .for_each(|toc_entry| hasher.update(toc_entry.as_bytes().as_slice()));
        hasher.update(self.data.as_slice());
        hasher.finalize()
    }

    pub fn save_to_file(&mut self, mut file: File) -> io::Result<usize> {
        self.file_header.checksum = self.checksum();
        file.write(self.file_header.as_bytes().as_slice())?;
        file.write(self.get_data_header().as_bytes().as_slice())?;
        for toc_entry in self.toc.as_slice() {
            file.write(toc_entry.as_bytes().as_slice())?;
        }
        file.write(self.data.as_slice())
    }

    pub fn from_file(mut file: File) -> io::Result<Self> {
        let magic = file.read_u32::<LittleEndian>()?;
        if magic != MAGIC {
            return Err(io::Error::from(ErrorKind::Unsupported));
        }
        let checksum = file.read_u32::<LittleEndian>()?;
        let version: u32 = file.read_u32::<LittleEndian>()?;
        let type_id: u32 = file.read_u32::<LittleEndian>()?;
        let toc_size: u32 = file.read_u32::<LittleEndian>()?;
        let mut reserved = [0u32; 11];
        file.read_u32_into::<LittleEndian>(&mut reserved)?;
        let mut container = Self {
            file_header: FileHeader::new(checksum),
            data_header: DataHeader::new(version, type_id, toc_size, reserved),
            toc: Vec::new(),
            data: Vec::new(),
        };

        for _ in 0..toc_size {
            let toc_entry = TocEntry::new_with_timestamp(
                file.read_u32::<LittleEndian>()?,
                file.read_u32::<LittleEndian>()?,
                file.read_u64::<LittleEndian>()?,
            );
            container.toc.push(toc_entry)
        }
        file.read_to_end(&mut container.data)?;
        if container.checksum() != container.file_header.checksum {
            return Err(io::Error::from(ErrorKind::InvalidData));
        }
        Ok(container)
    }
}

fn as_u8_slice<T>(v: &[T]) -> &[u8] {
    unsafe {
        std::slice::from_raw_parts(v.as_ptr() as *const u8, v.len() * std::mem::size_of::<T>())
    }
}
