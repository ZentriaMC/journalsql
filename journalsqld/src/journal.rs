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

    pub fn transport(&self) -> Option<String> {
        self.fields.get("_TRANSPORT").map(|field| field.into())
    }

    pub fn take_transport(&mut self) -> Option<String> {
        self.fields.remove("_TRANSPORT").map(|field| field.into())
    }

    pub fn hostname(&self) -> Option<String> {
        self.fields.get("_HOSTNAME").map(|field| field.into())
    }

    pub fn take_hostname(&mut self) -> Option<String> {
        self.fields.remove("_HOSTNAME").map(|field| field.into())
    }

    pub fn machine_id(&self) -> Option<String> {
        self.fields.get("_MACHINE_ID").map(|field| field.into())
    }

    pub fn take_machine_id(&mut self) -> Option<String> {
        self.fields.remove("_MACHINE_ID").map(|field| field.into())
    }

    pub fn boot_id(&self) -> Option<String> {
        self.fields.get("_BOOT_ID").map(|field| field.into())
    }

    pub fn take_boot_id(&mut self) -> Option<String> {
        self.fields.remove("_BOOT_ID").map(|field| field.into())
    }

    fn parse_realtime_timerstamp(
        entry: &JournalFieldValue,
    ) -> Result<time::OffsetDateTime, ParseIntError> {
        let micros = String::from(entry).parse::<i128>()?;

        Ok(time::OffsetDateTime::from_unix_timestamp_nanos(micros * 1000).unwrap())
    }

    pub fn realtime_timestamp(&self) -> Option<Result<time::OffsetDateTime, ParseIntError>> {
        self.fields
            .get("__REALTIME_TIMESTAMP")
            .map(Self::parse_realtime_timerstamp)
    }

    pub fn take_realtime_timestamp(
        &mut self,
    ) -> Option<Result<time::OffsetDateTime, ParseIntError>> {
        self.fields
                .remove("__REALTIME_TIMESTAMP")
                .map(|v| Self::parse_realtime_timerstamp(&v))
    }

    pub fn cursor(&self) -> Option<String> {
        self.fields.get("__CURSOR").map(|field| field.into())
    }

    pub fn take_cursor(&mut self) -> Option<String> {
        self.fields.remove("__CURSOR").map(|field| field.into())
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
