CREATE TABLE IF NOT EXISTS users (
    id         UUID,
    email      String,
    name       String,
    is_active  UInt8,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY id;
