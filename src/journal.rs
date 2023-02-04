use std::{collections::HashMap, num::ParseIntError};

#[cfg(feature = "bytes-as-base64")]
use base64::{engine::general_purpose::STANDARD as b64, Engine};

use serde::ser::SerializeMap;
use serde::Serialize;

#[derive(Clone, Debug)]
pub enum JournalFieldData {
    UTF8(String),
    Bytes(Vec<u8>),
}

impl From<&JournalFieldData> for String {
    fn from(value: &JournalFieldData) -> Self {
        match value {
            JournalFieldData::UTF8(value) => value.clone(),

            #[cfg(feature = "bytes-as-base64")]
            JournalFieldData::Bytes(value) => {
                let mut v = String::from("base64:");
                v += b64.encode(value).as_str();
                v
            }

            #[cfg(not(feature = "bytes-as-base64"))]
            JournalFieldData::Bytes(value) => {
                String::from_utf8_lossy(&strip_ansi_escapes::strip(value)).to_string()
            }
        }
    }
}

impl Serialize for JournalFieldData {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::UTF8(value) => serializer.serialize_str(value),

            #[cfg(feature = "bytes-as-base64")]
            Self::Bytes(value) => {
                let encoded = b64.encode(value);
                serializer.serialize_str(&encoded)
            }

            #[cfg(not(feature = "bytes-as-base64"))]
            Self::Bytes(value) => serializer.collect_seq(value.iter().map(|b| i32::from(*b))),
        }
    }
}

#[derive(Clone, Debug)]
pub struct JournalField {
    pub key: String,
    pub value: JournalFieldData,
}

#[derive(Debug)]
pub struct JournalEntry {
    pub(super) fields: HashMap<String, JournalFieldData, fnv::FnvBuildHasher>,
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
    pub fn put(&mut self, key: String, value: JournalFieldData) -> bool {
        self.fields.insert(key, value).is_some()
    }

    pub fn get<S: Into<String>>(&self, key: S) -> Option<&JournalFieldData> {
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
            .map(|field| String::from(field))
            .map(|value| value.parse::<i128>());

        if micros.is_none() {
            return None;
        }

        let micros = micros.unwrap();
        if micros.is_err() {
            return Some(Err(micros.unwrap_err()));
        }

        let micros = micros.unwrap();
        let value = time::OffsetDateTime::from_unix_timestamp_nanos(micros * 1000).unwrap();

        return Some(Ok(value));
    }
}

impl Default for JournalEntry {
    fn default() -> Self {
        Self {
            fields: HashMap::with_capacity_and_hasher(16, fnv::FnvBuildHasher::default()),
        }
    }
}
