CREATE TABLE IF NOT EXISTS trades (
    id        UUID,
    symbol    String,
    side      String,
    price     Float64,
    volume    Int64,
    timestamp DateTime
) ENGINE = MergeTree()
ORDER BY (timestamp, symbol);
