use std::{fs::OpenOptions, io::{BufWriter, Seek, Write}, path::Path};

use crate::page::{Page, SIZE};

pub struct Writer {

}

impl Writer {
    pub fn write(pathname: &str, page: &Page) {
        let file = OpenOptions::new().write(true).create(true).open(pathname).unwrap();

        let mut buffer_writer = BufWriter::with_capacity(SIZE, file);

        buffer_writer.write_all(&*page.data);
        buffer_writer.flush();
    }
}