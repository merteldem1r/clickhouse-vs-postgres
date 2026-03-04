CREATE TABLE IF NOT EXISTS trades (
    id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    symbol    VARCHAR(20) NOT NULL,
    side      VARCHAR(4) NOT NULL,
    price     DOUBLE PRECISION NOT NULL,
    volume    BIGINT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL
);
CREATE INDEX idx_trades_timestamp ON trades(timestamp);
CREATE INDEX idx_trades_symbol ON trades(symbol);
