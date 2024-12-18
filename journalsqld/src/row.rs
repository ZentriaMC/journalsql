use std::collections::HashSet;

use anyhow::Context;
use clickhouse::Row;
use lazy_static::lazy_static;
use log::trace;
use serde::Serialize;

use crate::journal::JournalEntry;

lazy_static! {
    static ref INSERT_IGNORED_FIELDS: HashSet<&'static str> = {
        let mut ignored_fields: HashSet<&'static str> = HashSet::new();
        ignored_fields.insert("_BOOT_ID");
        ignored_fields.insert("_HOSTNAME");
        ignored_fields.insert("_MACHINE_ID");
        ignored_fields.insert("_TRANSPORT");
        ignored_fields.insert("__CURSOR");
        ignored_fields.insert("__REALTIME_TIMESTAMP");

        // These fields are in __CURSOR
        ignored_fields.insert("__SEQNUM");
        ignored_fields.insert("__SEQNUM_ID");
        ignored_fields.insert("__MONOTONIC_TIMESTAMP");

        ignored_fields
    };
}

type Error = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, thiserror::Error)]
pub enum RowCreateError {
    #[error("Missing required field \"{field}\"")]
    MissingField { field: String },

    #[error("{0}")]
    Unspecified(Error),
}

impl RowCreateError {
    pub fn missing_field<S: Into<String>>(field: S) -> Self {
        Self::MissingField {
            field: field.into(),
        }
    }
}

#[derive(Serialize, Row)]
pub struct LogRecordRow {
    pub machine_id: String,
    pub boot_id: String,
    // systemd timestamps are in microseconds
    #[serde(with = "clickhouse::serde::time::datetime64::micros")]
    pub timestamp: time::OffsetDateTime,
    pub hostname: String,
    pub transport: String,
    pub cursor: String,
    // Map(String, String)
    pub record: Vec<(String, String)>,
}

impl TryFrom<JournalEntry> for LogRecordRow {
    type Error = RowCreateError;

    fn try_from(mut value: JournalEntry) -> Result<Self, Self::Error> {
        // Grab common fields
        let transport = value
            .take_transport()
            .context("no transport supplied")
            .map_err(|_e| RowCreateError::missing_field("_TRANSPORT"))?;

        let machine_id = value
            .take_machine_id()
            .context("no machine id supplied")
            .map_err(|_e| RowCreateError::missing_field("_MACHINE_ID"))?;

        let boot_id = value
            .take_boot_id()
            .context("no boot id supplied")
            .map_err(|_e| RowCreateError::missing_field("_BOOT_ID"))?;

        let hostname = value
            .take_hostname()
            .context("no hostname supplied")
            .map_err(|_e| RowCreateError::missing_field("_HOSTNAME"))?;

        let timestamp = value
            .take_realtime_timestamp()
            .context("no timestamp supplied")
            .map_err(|_e| RowCreateError::missing_field("__REALTIME_TIMESTAMP"))?
            .map_err(|e| RowCreateError::Unspecified(e.into()))?;

        let cursor = value
            .take_cursor()
            .context("no cursor supplied")
            .map_err(|_e| RowCreateError::missing_field("__CURSOR"))?;

        if log::log_enabled!(log::Level::Trace) {
            trace!(
                "entry: {}",
                serde_json::to_string_pretty(&value)
                    .map_err(|e| RowCreateError::Unspecified(e.into()))?
            );
        }

        let mut record: Vec<(String, String)> = Vec::with_capacity(value.fields.len());
        for (key, field) in value.fields.into_iter() {
            if INSERT_IGNORED_FIELDS.contains(key.as_str()) {
                continue;
            }

            record.push((key, field.into()));
        }

        Ok(LogRecordRow {
            machine_id,
            timestamp,
            boot_id,
            hostname,
            transport,
            cursor,
            record,
        })
    }
}
