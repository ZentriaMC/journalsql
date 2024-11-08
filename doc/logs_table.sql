-- Order:
-- 1) machine id - one machine
-- 2) boot id - multiple boot ids per machine
-- 3) timestamp - multiple messages per machine boot

CREATE TABLE IF NOT EXISTS logs2 (
    `machine_id` LowCardinality(String),
    `boot_id` LowCardinality(String),
    `timestamp` DateTime64(6) CODEC(DoubleDelta, ZSTD),
    `hostname` LowCardinality(String),
    `transport` LowCardinality(String),
    `cursor` String CODEC(LZ4),
    `record` Map(LowCardinality(String), String)
)
ENGINE = MergeTree
PARTITION BY (toStartOfHour(`timestamp`), `machine_id`, `boot_id`)
ORDER BY (`timestamp`)
;
