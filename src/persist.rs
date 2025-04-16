use std::io::{BufReader, BufWriter, Error, Read, Seek, Write};
use std::os::unix::fs::FileExt;
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
    buf_reader: BufReader<File>,
}

impl Reader {
    pub fn new (path: &str, filename: &str) -> Reader {
        let file = OpenOptions::new()
            .read(true)
            .open(Path::new(path).join(filename))
            .expect("Cannot open a file for reading");

        let file2 = OpenOptions::new()
            .read(true)
            .open(Path::new(path).join(filename))
            .expect("Cannot open a file for reading");

        Reader {
            file,
            buf_reader: BufReader::new(file2),
        }
    }

    pub fn page_count(&self) -> u64 {
        self.file.metadata().unwrap().len() / (SIZE as u64)
    }

    pub fn read_page(&mut self, page_id: u64) -> Result<Page, Error> {
        let mut page_data = [0u8; SIZE];

        self.file.read_exact_at(&mut page_data, page_id * (SIZE as u64))?;

        Ok(Page::from_data(page_id, page_data))
    }

    pub fn seek_to_page(&mut self, page_id: u64) -> Result<bool, Error> {
        self.buf_reader.seek(std::io::SeekFrom::Start(page_id * (SIZE as u64)))?;

        Ok(true)
    }

    pub fn read_page_sequentially(&mut self, page_id: u64) -> Result<Vec<Page>, Error> {
        let mut page_data = [0u8; 4 * SIZE];

        self.buf_reader.read(&mut page_data)?;

        let mut pid = page_id;

        Ok(page_data
            .chunks(SIZE)
            .map(| data | {
                let p = Page::from_data(pid, data.try_into().unwrap());

                pid += 1;

                p
            })
            .collect::<Vec<Page>>()
        )
    }
}