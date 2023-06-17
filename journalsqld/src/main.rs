use std::os::fd::{AsFd, AsRawFd, FromRawFd};

use std::time::Duration;

use anyhow::Context;
use clickhouse::inserter::Inserter;
use crossbeam_channel::{select, Receiver};
use log::{debug, error, trace, warn};
use row::LogRecordRow;
use signal_hook::{consts::{SIGINT, SIGTERM}, iterator::Signals};
use time::OffsetDateTime;

mod journal;
mod metrics;
mod row;
mod util;

use journal::{read_journal_entries, JournalEntry};

type Error = Box<dyn std::error::Error + Send + Sync>;

#[tokio::main]
async fn main() {
    env_logger::init();

    if let Err(err) = entrypoint().await {
        panic!("unhandled error: {:?}", err)
    }
}

fn sigint_notifier() -> Result<Receiver<()>, Error> {
    let (sender, receiver) = crossbeam_channel::bounded::<()>(32);

    let mut signals = Signals::new([SIGINT, SIGTERM])?;

    std::thread::spawn(move || {
        for sig in signals.forever() {
            debug!("got signal {}", sig);
            if sender.send(()).is_err() {
                break;
            }
        }
    });

    Ok(receiver)
}

async fn entrypoint() -> Result<(), Error> {
    let db = clickhouse::Client::default()
        .with_compression(clickhouse::Compression::Lz4)
        .with_database("default")
        .with_url("http://127.0.0.1:18123");

    let mut logs_inserter: Inserter<LogRecordRow> = db
        .inserter("logs2")?
        .with_max_entries(100_000)
        .with_period(Some(Duration::from_secs(5)));

    // let sigint_ch = sigint_notifier()?;
    let machines = 1;
    let (entry_sender, entry_receiver) =
        crossbeam_channel::bounded::<JournalEntry>(4 * num_cpus::get() * machines);

    let consumer_fut = async move {
        let receiver = entry_receiver;

        'the_loop: loop {
            select! {
                // TODO: async is hard, logs_inserter.insert starts complaining
                // recv(sigint_ch) -> _ => {
                //     break 'the_loop;
                // },

                recv(receiver) -> entry => {
                    let entry = match entry {
                        Ok(entry) => entry,
                        Err(err) => {
                            trace!("channel recv err: {:?}", err);
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

                    // TODO: this sucks
                    let ts_diff = current_timestamp - row.timestamp;
                    if ts_diff.is_positive() && ts_diff.whole_seconds() > 5 {
                        warn!("LAG! unable to keep up - recv diff: {}", ts_diff);
                    }

                    // Insert
                    logs_inserter.write(&row).await?;
                    let res = logs_inserter.commit().await?;

                    if res.entries > 0 {
                        debug!("inserted={} txns={}", res.entries, res.transactions);
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
        // Use raw fd for input to avoid expensive buffering until next newline
        let stdin = std::io::stdin().lock();
        let fd = stdin.as_fd();
        let mut file = unsafe { std::fs::File::from_raw_fd(fd.as_raw_fd()) };

        read_journal_entries(&mut file, entry_sender).context("failed to read entries")
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
