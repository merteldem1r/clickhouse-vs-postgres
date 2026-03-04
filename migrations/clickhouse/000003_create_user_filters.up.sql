CREATE TABLE IF NOT EXISTS user_filters (
    id         UUID,
    user_id    UUID,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),
    filters    String
) ENGINE = MergeTree()
ORDER BY user_id;
