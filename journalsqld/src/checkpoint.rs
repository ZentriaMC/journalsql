use std::{
    io::{self, SeekFrom},
    path::Path,
};

use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::Mutex,
};

use crate::Error;

pub struct Checkpointer {
    file: Mutex<File>,
    current: Mutex<Option<String>>,
}

impl Checkpointer {
    pub async fn new<P: AsRef<Path>>(file: P) -> Result<Self, io::Error> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(file)
            .await?;

        Ok(Self {
            file: Mutex::new(file),
            current: Mutex::new(None),
        })
    }

    pub async fn get(&self) -> Result<Option<String>, Error> {
        let mut current = self.current.lock().await;

        if let Some(ref current) = current.as_deref() {
            return Ok(Some(current.to_string()));
        }

        // Try reading from file
        let mut file = self.file.lock().await;
        file.seek(SeekFrom::Start(0)).await?;

        let mut cursor = String::new();
        file.read_to_string(&mut cursor).await?;
        cursor = cursor.trim().to_string();

        if cursor.is_empty() {
            return Ok(None);
        }

        current.replace(cursor.clone());
        Ok(Some(cursor))
    }

    pub async fn set(&self, cursor: &str) -> Result<(), Error> {
        let mut current = self.current.lock().await;
        current.replace(cursor.to_string());

        let file = self.file.lock().await;
        tokio::pin!(file);

        file.seek(SeekFrom::Start(0)).await?;
        file.write_all(format!("{}\n", cursor).as_bytes()).await?;

        Ok(())
    }
}
