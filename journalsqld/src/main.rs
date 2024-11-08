use std::os::fd::{AsFd, AsRawFd, FromRawFd};
use std::time::Duration;

use anyhow::Context;
use clickhouse::inserter::Inserter;
use log::{debug, error, info, trace};
use row::LogRecordRow;
use signal_hook::{
    consts::{SIGINT, SIGTERM},
    iterator::Signals,
};
use time::OffsetDateTime;
use tokio::sync::{broadcast, mpsc};
use url::Url;

mod journal;
mod metrics;
mod row;
mod util;

use crate::journal::{read_journal_entries, JournalEntry};

type Error = Box<dyn std::error::Error + Send + Sync>;

#[tokio::main]
async fn main() {
    env_logger::init();

    if let Err(err) = entrypoint().await {
        panic!("unhandled error: {:?}", err)
    }
}

fn sigint_notifier() -> Result<broadcast::Receiver<()>, Error> {
    let (sender, receiver) = broadcast::channel::<()>(1);

    let mut signals = Signals::new([SIGINT, SIGTERM])?;

    std::thread::spawn(move || {
        if let Some(sig) = signals.forever().next() {
            debug!("got signal {}", sig);
            sender.send(()).expect("failed to send");
        }
    });

    Ok(receiver)
}

fn create_client(uri: &str) -> Result<clickhouse::Client, url::ParseError> {
    let mut uri: Url = uri.parse()?;
    let mut client = clickhouse::Client::default().with_compression(clickhouse::Compression::Lz4);

    if uri.username() != "" {
        client = client.with_user(uri.username());
    }

    if let Some(password) = uri.password() {
        client = client.with_password(password);
    }

    client = client.with_database(
        uri.path()
            .strip_prefix('/')
            .filter(|path| !path.is_empty())
            .unwrap_or("default"),
    );

    let _ = uri.set_username("");
    let _ = uri.set_password(None);
    uri.set_path("/");

    client = client.with_url(uri.to_string());
    Ok(client)
}

async fn entrypoint() -> Result<(), Error> {
    let clickhouse_uri = std::env::var("CLICKHOUSE_URI").expect("CLICKHOUSE_URI envvar is not set");
    let db = create_client(&clickhouse_uri)?;

    let mut logs_inserter: Inserter<LogRecordRow> = db
        .inserter("logs2")?
        .with_max_entries(100_000)
        .with_period(Some(Duration::from_secs(5)));

    let mut sigint_ch = sigint_notifier()?;
    let machines = 1;
    let (entry_sender, entry_receiver) =
        mpsc::channel::<JournalEntry>(4 * num_cpus::get() * machines);

    let consumer_fut = async move {
        let mut receiver = entry_receiver;

        'the_loop: loop {
            tokio::select! {
                _ = sigint_ch.recv() => {
                    break 'the_loop;
                },

                entry = receiver.recv() => {
                    let entry = match entry {
                        Some(entry) => entry,
                        None => {
                            trace!("we done");
                            break;
                        },
                    };

                    let current_timestamp = OffsetDateTime::now_utc();
                    let row = match LogRecordRow::try_from(&entry) {
                        Ok(row) => row,
                        Err(err) => {
                            error!("failed to produce row: {}", err);
                            metrics::inc_log_entries_unprocessed("unknown").unwrap();
                            continue;
                        }
                    };

                    metrics::inc_log_entries_processed(&row.hostname).unwrap();
                    metrics::set_last_received_entry_timestamp(&row.hostname, &row.timestamp).unwrap();
                    let ts_diff = current_timestamp - row.timestamp;

                    // Insert
                    logs_inserter.write(&row).await?;
                    let res = logs_inserter.commit().await?;

                    if res.entries > 0 {
                        if ts_diff.is_positive() && ts_diff.whole_seconds() > 5 {
                            info!("inserted={} txns={} behind={}", res.entries, res.transactions, ts_diff);
                        } else {
                            info!("inserted={} txns={}", res.entries, res.transactions);
                        }
                    }
                },
            }
        }

        logs_inserter
            .end()
            .await
            .context("failed to end logs inserter")
    };

    let producer_fut = async move {
        let stdin = {
            let stdin = std::io::stdin().lock();
            let fd = stdin.as_fd();
            unsafe { std::fs::File::from_raw_fd(fd.as_raw_fd()) }
        };

        read_journal_entries(Box::new(stdin), entry_sender)
            .await
            .context("failed to read entries")
    };

    let consumer = tokio::task::spawn(consumer_fut);
    let producer = tokio::task::spawn(producer_fut);
    let (consumer_res, producer_res) = tokio::try_join!(consumer, producer)?;

    if let Err(err) = consumer_res {
        debug!("consumer err={:?}", err);
    }

    if let Err(err) = producer_res {
        debug!("producer err={:?}", err);
    }

    if log::log_enabled!(log::Level::Debug) {
        let metrics = prometheus::gather();
        let encoded = prometheus::TextEncoder::new().encode_to_string(&metrics)?;
        debug!("final runtime metrics=\n{}", encoded);
    }

    Ok(())
}
