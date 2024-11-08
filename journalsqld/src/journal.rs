use std::io::Read;
use std::{collections::HashMap, num::ParseIntError};

use log::{debug, trace};
use serde::ser::SerializeMap;
use serde::Serialize;
use systemd_journal_parser::{parse_journal_field, JournalFieldValue};
use tokio::sync::mpsc;

use crate::metrics;
use crate::util::measure;

#[derive(Debug)]
pub struct JournalEntry {
    pub(super) fields: HashMap<String, JournalFieldValue, fnv::FnvBuildHasher>,
}

impl Serialize for JournalEntry {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.fields.len()))?;
        for (key, value) in self.fields.iter() {
            map.serialize_key(key)?;
            map.serialize_value(value)?;
        }

        map.end()
    }
}

impl JournalEntry {
    pub fn put(&mut self, key: String, value: JournalFieldValue) -> bool {
        self.fields.insert(key, value).is_some()
    }

    pub fn get<S: Into<String>>(&self, key: S) -> Option<&JournalFieldValue> {
        self.fields.get(&key.into())
    }

    pub fn transport(&self) -> Option<String> {
        self.get("_TRANSPORT").map(|field| field.into())
    }

    pub fn hostname(&self) -> Option<String> {
        self.get("_HOSTNAME").map(|field| field.into())
    }

    pub fn machine_id(&self) -> Option<String> {
        self.get("_MACHINE_ID").map(|field| field.into())
    }

    pub fn boot_id(&self) -> Option<String> {
        self.get("_BOOT_ID").map(|field| field.into())
    }

    pub fn realtime_timestamp(&self) -> Option<Result<time::OffsetDateTime, ParseIntError>> {
        let micros = self
            .get("__REALTIME_TIMESTAMP")
            .map(String::from)
            .map(|value| value.parse::<i128>());

        micros.as_ref()?;

        let micros = micros.unwrap();
        if let Err(err) = micros {
            return Some(Err(err));
        }

        let micros = micros.unwrap();
        let value = time::OffsetDateTime::from_unix_timestamp_nanos(micros * 1000).unwrap();

        Some(Ok(value))
    }

    pub fn cursor(&self) -> Option<String> {
        self.get("__CURSOR").map(|field| field.into())
    }
}

impl Default for JournalEntry {
    fn default() -> Self {
        Self {
            fields: HashMap::with_capacity_and_hasher(16, fnv::FnvBuildHasher::default()),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum JournalReadError {
    #[error("I/O error")]
    IOError(std::io::Error),

    #[error("Parse error")]
    ParseError(nom::error::ErrorKind, Vec<u8>),
}

pub async fn read_journal_entries(
    mut reader: Box<impl std::io::Read + Send>,
    sender: mpsc::Sender<JournalEntry>,
) -> Result<(), JournalReadError> {
    let mut current_entry = JournalEntry::default();
    let mut input = Vec::with_capacity(8192);

    const READ_STEP: usize = 1;

    loop {
        let (elapsed, parse_result) = measure(|| parse_journal_field(&input));
        metrics::set_last_entry_parse_time(elapsed).unwrap();

        let to_read = match parse_result {
            Ok((remaining, parsed)) => {
                trace!("processed={:?}", parsed);

                let remaining = Vec::from(remaining);
                input.truncate(remaining.len());
                input.extend(&remaining);

                current_entry.put(parsed.key, parsed.value);

                READ_STEP
            }
            Err(nom::Err::Incomplete(nom::Needed::Unknown)) => READ_STEP,
            Err(nom::Err::Incomplete(nom::Needed::Size(sz))) => sz.get(),
            Err(nom::Err::Error(e)) => {
                if e.code == nom::error::ErrorKind::Eof {
                    // If we've hit an eof and have only newline in the buffer, then it's end of the journal entry
                    if input.len() == 1 && input[0] == b'\n' {
                        if let Err(err) = sender.send(current_entry).await {
                            debug!("producer channel closed: {:?}", err);
                            break;
                        }

                        current_entry = JournalEntry::default();
                        input.truncate(0);
                    }

                    READ_STEP
                } else {
                    return Err(JournalReadError::ParseError(e.code, e.input.to_owned()));
                }
            }
            Err(nom::Err::Failure(e)) => {
                return Err(JournalReadError::ParseError(e.code, e.input.to_owned()));
            }
        };

        reader
            .as_mut()
            .take(to_read as u64)
            .read_to_end(&mut input)
            .map_err(JournalReadError::IOError)?;

        if input.is_empty() {
            break;
        }
    }

    Ok(())
}
