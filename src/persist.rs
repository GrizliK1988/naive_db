use std::io::{BufReader, BufWriter, Error, Read, Seek, Write};
use std::path::{ Path, PathBuf };
use std::fs::{ File, OpenOptions };
use crate::page::{Page, SIZE};

const WRITE_BUFFER_SIZE: usize = 8 * 1024;
const READ_BUFFER_SIZE: usize = 8 * 1024;

pub struct Writer {
    path: PathBuf,
}

impl Writer {
    pub fn new (path: &str, filename: &str) -> Writer {
        Writer {
            path: Path::new(path).join(filename),
        }
    }

    pub fn insert_page(&self, page: &Page) -> Result<(), Error> {
        let mut file = self.open_write_file()?;

        file.seek(std::io::SeekFrom::End(0))?;

        let mut buf_writer = BufWriter::with_capacity(WRITE_BUFFER_SIZE, file);
        buf_writer.write_all(&*page.data)?;
        buf_writer.flush()?;

        Ok(())
    }

    fn open_write_file(&self) -> Result<File, Error> {
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&self.path)
    }
}

pub struct Reader {
    path: PathBuf,
}

impl Reader {
    pub fn new (path: &str, filename: &str) -> Reader {
        Reader {
            path: Path::new(path).join(filename),
        }
    }

    pub fn read_page(&self, page_id: u64) -> Result<Page, Error> {
        let mut file = self.open_read_file()?;

        file.seek(std::io::SeekFrom::Start(page_id * (SIZE as u64)))?;

        let mut buf_reader = BufReader::with_capacity(READ_BUFFER_SIZE, file);

        let mut page_data = [0u8; SIZE];

        buf_reader.read_exact(&mut page_data)?;

        Ok(Page::from_data(page_data))
    }

    fn open_read_file(&self) -> Result<File, Error> {
        OpenOptions::new()
            .read(true)
            .open(&self.path)
    }
}