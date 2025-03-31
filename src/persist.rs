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
    file: File,
}

impl Reader {
    pub fn new (path: &str, filename: &str) -> Reader {
        Reader {
            file: OpenOptions::new()
                .read(true)
                .open(Path::new(path).join(filename))
                .expect("Cannot open a file for reading")
        }
    }

    pub fn page_count(&self) -> u64 {
        self.file.metadata().unwrap().len() / (SIZE as u64)
    }

    pub fn read_page(&mut self, page_id: u64) -> Result<Page, Error> {
        self.file.seek(std::io::SeekFrom::Start(page_id * (SIZE as u64)))?;

        let mut buf_reader = BufReader::with_capacity(READ_BUFFER_SIZE, &self.file);

        let mut page_data = [0u8; SIZE];

        buf_reader.read_exact(&mut page_data)?;

        Ok(Page::from_data(page_data))
    }
}