CREATE TABLE IF NOT EXISTS user_filters (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id    UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    filters    JSONB NOT NULL DEFAULT '{}'
);
CREATE INDEX idx_user_filters_user_id ON user_filters(user_id);
