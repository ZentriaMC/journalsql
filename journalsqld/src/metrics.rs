use std::convert::TryFrom;
use std::{num::TryFromIntError, time::Duration};

use lazy_static::lazy_static;
use prometheus::{
    register_histogram, register_int_counter_vec, register_int_gauge_vec, Histogram, IntCounterVec,
    IntGaugeVec,
};

pub const LABEL_HOSTNAME: &'static str = "hostname";

lazy_static! {
    pub static ref LOG_ENTRIES_PROCESSED: IntCounterVec = register_int_counter_vec!(
        "journal_entries_processed",
        "Total number of journal entries processed during runtime",
        &[LABEL_HOSTNAME]
    )
    .unwrap();
    pub static ref LOG_ENTRIES_UNPROCESSABLE: IntCounterVec = register_int_counter_vec!(
        "journal_entries_unprocessable",
        "Total number of journal entries which weren't processable due to an error",
        &[LABEL_HOSTNAME]
    )
    .unwrap();
    pub static ref LAST_RECEIVED_ENTRY_TIMESTAMP: IntGaugeVec = register_int_gauge_vec!(
        "journal_last_received_timestamp",
        "Last received journal entry timestamp",
        &[LABEL_HOSTNAME]
    )
    .unwrap();
    pub static ref LAST_ENTRY_PARSE_TIME: Histogram = register_histogram!(
        "journal_last_entry_parse_time",
        "Last journal entry parse time in microseconds"
    )
    .unwrap();
}

pub fn inc_log_entries_processed(hostname: &str) -> Result<(), prometheus::Error> {
    let metric = LOG_ENTRIES_PROCESSED.get_metric_with_label_values(&[hostname])?;
    metric.inc();

    Ok(())
}

pub fn inc_log_entries_unprocessed(hostname: &str) -> Result<(), prometheus::Error> {
    let metric = LOG_ENTRIES_UNPROCESSABLE.get_metric_with_label_values(&[hostname])?;
    metric.inc();

    Ok(())
}

pub fn set_last_received_entry_timestamp(
    hostname: &str,
    timestamp: &time::OffsetDateTime,
) -> Result<(), prometheus::Error> {
    let metric = LAST_RECEIVED_ENTRY_TIMESTAMP.get_metric_with_label_values(&[hostname])?;
    let millis = (timestamp.unix_timestamp() * 1000) + timestamp.millisecond() as i64;
    metric.set(millis);

    Ok(())
}

pub fn set_last_entry_parse_time(duration: Duration) -> Result<(), TryFromIntError> {
    let nanos = u32::try_from(duration.as_nanos())?;
    let micros = f64::from(nanos) / 1000.0;
    LAST_ENTRY_PARSE_TIME.observe(micros);

    Ok(())
}
