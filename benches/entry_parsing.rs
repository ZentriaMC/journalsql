use std::fs::File;

use criterion::{criterion_group, criterion_main, Criterion};

use journalsql::journal::{read_journal_entries, JournalEntry};

fn entry_parsing_benchmark(c: &mut Criterion) {
    let mut parse_group = c.benchmark_group("parsing");
    parse_group
        .sample_size(20)
        .bench_function("parse entries 300", |b| {
            b.iter(|| {
                let mut collected = Vec::with_capacity(300);

                let (tx, rx) = crossbeam_channel::bounded::<JournalEntry>(32);

                std::thread::spawn(|| {
                    let mut f = File::open("./benches/data/300_messages.bin").unwrap();
                    read_journal_entries(&mut f, tx).unwrap();
                });

                while let Ok(value) = rx.recv() {
                    collected.push(value);
                }
            })
        });

    parse_group.finish();
}

criterion_group!(benches, entry_parsing_benchmark);
criterion_main!(benches);
