use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::path::Path;

use bincode::config::{self, Config};
use bincode::serde::{decode_from_slice, encode_into_slice, encode_to_vec};
use common::{DbError, DbResult, PageId, RecordId, Row};

pub const PAGE_SIZE: usize = 4096;
const HEADER_BYTES: usize = size_of::<PageHeader>();
const SLOT_BYTES: usize = size_of::<Slot>();

fn bincode_config() -> impl Config {
    config::legacy()
}

#[derive(Debug, Clone)]
pub struct Page {
    pub id: u64,
    pub data: Vec<u8>,
}

impl Page {
    pub fn new(id: u64) -> Self {
        let mut page = Self {
            id,
            data: vec![0u8; PAGE_SIZE],
        };
        page.write_header(&PageHeader::default())
            .expect("initialize page header");
        page
    }

    fn header(&self) -> DbResult<PageHeader> {
        let (header, read) = decode_from_slice(&self.data[..HEADER_BYTES], bincode_config())
            .map_err(|e| DbError::Storage(format!("read page header failed: {e}")))?;
        debug_assert_eq!(read, HEADER_BYTES);
        Ok(header)
    }

    fn write_header(&mut self, header: &PageHeader) -> DbResult<()> {
        let written = encode_into_slice(header, &mut self.data[..HEADER_BYTES], bincode_config())
            .map_err(|e| DbError::Storage(format!("write page header failed: {e}")))?;
        debug_assert_eq!(written, HEADER_BYTES);
        Ok(())
    }

    fn slot_offset(slot_idx: u16) -> usize {
        HEADER_BYTES + slot_idx as usize * SLOT_BYTES
    }

    fn read_slot(&self, slot_idx: u16) -> DbResult<Slot> {
        let start = Self::slot_offset(slot_idx);
        let end = start + SLOT_BYTES;
        if end > PAGE_SIZE {
            return Err(DbError::Storage(format!("slot {slot_idx} out of bounds")));
        }
        let (slot, read) = decode_from_slice(&self.data[start..end], bincode_config())
            .map_err(|e| DbError::Storage(format!("read slot failed: {e}")))?;
        debug_assert_eq!(read, SLOT_BYTES);
        Ok(slot)
    }

    fn write_slot(&mut self, slot_idx: u16, slot: &Slot) -> DbResult<()> {
        let start = Self::slot_offset(slot_idx);
        let end = start + SLOT_BYTES;
        if end > PAGE_SIZE {
            return Err(DbError::Storage(format!("slot {slot_idx} out of bounds")));
        }
        let written = encode_into_slice(slot, &mut self.data[start..end], bincode_config())
            .map_err(|e| DbError::Storage(format!("write slot failed: {e}")))?;
        debug_assert_eq!(written, SLOT_BYTES);
        Ok(())
    }

    fn free_space(&self) -> DbResult<usize> {
        let header = self.header()?;
        let slots_start = HEADER_BYTES + header.num_slots as usize * SLOT_BYTES;
        let free_offset = usize::from(header.free_offset);
        Ok(free_offset.saturating_sub(slots_start))
    }

    fn can_fit(&self, payload_len: usize) -> DbResult<bool> {
        let needed = payload_len + SLOT_BYTES;
        Ok(self.free_space()? >= needed)
    }

    fn append_tuple(&mut self, bytes: &[u8]) -> DbResult<u16> {
        if bytes.len() > u16::MAX as usize {
            return Err(DbError::Storage("row exceeds maximum tuple size".into()));
        }
        let mut header = self.header()?;
        if header.num_slots == u16::MAX {
            return Err(DbError::Storage("slot index overflow".into()));
        }
        if !self.can_fit(bytes.len())? {
            return Err(DbError::Storage("page full".into()));
        }
        let slot_idx = header.num_slots;
        let len = bytes.len() as u16;
        let new_free_offset = header.free_offset - len;
        self.data[new_free_offset as usize..header.free_offset as usize].copy_from_slice(bytes);

        let slot = Slot {
            offset: new_free_offset,
            len,
        };
        self.write_slot(slot_idx, &slot)?;

        header.num_slots += 1;
        header.free_offset = new_free_offset;
        self.write_header(&header)?;
        Ok(slot_idx)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PageHeader {
    pub num_slots: u16,
    pub free_offset: u16,
}

impl Default for PageHeader {
    fn default() -> Self {
        Self {
            num_slots: 0,
            free_offset: PAGE_SIZE as u16,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Slot {
    pub offset: u16,
    pub len: u16,
}

impl Slot {
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

pub trait HeapTable {
    fn insert(&mut self, row: &Row) -> DbResult<RecordId>;
    fn get(&mut self, rid: RecordId) -> DbResult<Row>;
    fn update(&mut self, rid: RecordId, row: &Row) -> DbResult<()>;
    fn delete(&mut self, rid: RecordId) -> DbResult<()>;
}

#[derive(Debug)]
pub struct HeapFile {
    file: File,
    pub table_id: u64,
}

impl HeapFile {
    pub fn open(path: &Path, table_id: u64) -> DbResult<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        Ok(Self { file, table_id })
    }

    fn file_len(&self) -> DbResult<u64> {
        Ok(self.file.metadata()?.len())
    }

    fn num_pages(&self) -> DbResult<u64> {
        Ok(self.file_len()? / PAGE_SIZE as u64)
    }

    fn last_page_id(&self) -> DbResult<Option<u64>> {
        let pages = self.num_pages()?;
        if pages == 0 {
            Ok(None)
        } else {
            Ok(Some(pages - 1))
        }
    }

    fn allocate_page(&self) -> DbResult<Page> {
        let id = self.num_pages()?;
        Ok(Page::new(id))
    }

    fn read_page(&mut self, page_id: u64) -> DbResult<Page> {
        let mut page = Page::new(page_id);
        if page_id >= self.num_pages()? {
            return Ok(page);
        }

        self.file
            .seek(SeekFrom::Start(page_id * PAGE_SIZE as u64))?;
        self.file.read_exact(&mut page.data)?;
        Ok(page)
    }

    fn write_page(&mut self, page: &Page) -> DbResult<()> {
        self.file
            .seek(SeekFrom::Start(page.id * PAGE_SIZE as u64))?;
        self.file.write_all(&page.data)?;
        self.file.flush()?;
        Ok(())
    }

    fn ensure_page_exists(&self, page_id: u64) -> DbResult<()> {
        if page_id >= self.num_pages()? {
            return Err(DbError::Storage(format!("page {page_id} not allocated")));
        }
        Ok(())
    }
}

impl HeapTable for HeapFile {
    fn insert(&mut self, row: &Row) -> DbResult<RecordId> {
        let bytes = encode_to_vec(row, bincode_config())
            .map_err(|e| DbError::Storage(format!("serialize row failed: {e}")))?;

        let mut page = match self.last_page_id()? {
            Some(id) => self.read_page(id)?,
            None => self.allocate_page()?,
        };

        if !page.can_fit(bytes.len())? {
            page = self.allocate_page()?;
        }

        let slot = page.append_tuple(&bytes)?;
        self.write_page(&page)?;

        Ok(RecordId {
            page_id: PageId(page.id),
            slot,
        })
    }

    fn get(&mut self, rid: RecordId) -> DbResult<Row> {
        self.ensure_page_exists(rid.page_id.0)?;
        let page = self.read_page(rid.page_id.0)?;
        let header = page.header()?;
        if rid.slot >= header.num_slots {
            return Err(DbError::Storage(format!("invalid slot {}", rid.slot)));
        }
        let slot = page.read_slot(rid.slot)?;
        if slot.is_empty() {
            return Err(DbError::Storage("slot empty".into()));
        }
        let start = slot.offset as usize;
        let end = start + slot.len as usize;
        let (row, _) = decode_from_slice(&page.data[start..end], bincode_config())
            .map_err(|e| DbError::Storage(format!("deserialize row failed: {e}")))?;
        Ok(row)
    }

    fn update(&mut self, rid: RecordId, row: &Row) -> DbResult<()> {
        self.delete(rid)?;
        self.insert(row)?;
        Ok(())
    }

    fn delete(&mut self, rid: RecordId) -> DbResult<()> {
        self.ensure_page_exists(rid.page_id.0)?;
        let mut page = self.read_page(rid.page_id.0)?;
        let header = page.header()?;
        if rid.slot >= header.num_slots {
            return Err(DbError::Storage(format!("invalid slot {}", rid.slot)));
        }
        let mut slot = page.read_slot(rid.slot)?;
        if slot.is_empty() {
            return Err(DbError::Storage("slot already empty".into()));
        }
        slot.len = 0;
        page.write_slot(rid.slot, &slot)?;
        self.write_page(&page)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
