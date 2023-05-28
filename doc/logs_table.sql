CREATE TABLE IF NOT EXISTS logs2 (
    `timestamp` DateTime64(6) CODEC(DoubleDelta, ZSTD),
    `machine_id` LowCardinality(String),
    `boot_id` LowCardinality(String),
    `hostname` LowCardinality(String),
    `transport` LowCardinality(String),
    `record` Map(LowCardinality(String), String)
)
ENGINE = MergeTree
PARTITION BY (toStartOfHour(`timestamp`), `machine_id`)
ORDER BY (`timestamp`)
;
