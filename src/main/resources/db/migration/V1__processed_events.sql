CREATE TABLE processed_events (
    event_id UUID PRIMARY KEY,
    schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    transaction_id BIGINT NOT NULL CHECK (transaction_id > 0),
    type VARCHAR(16) NOT NULL CHECK (type IN ('DEPOSIT', 'WITHDRAWAL', 'TRANSFER')),
    amount BIGINT NOT NULL CHECK (amount > 0),
    source_wallet_id BIGINT,
    destination_wallet_id BIGINT,
    occurred_at TIMESTAMPTZ NOT NULL,
    trace_id UUID NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT valid_wallet_sides CHECK (
        (type = 'DEPOSIT' AND source_wallet_id IS NULL AND destination_wallet_id > 0)
        OR (type = 'WITHDRAWAL' AND source_wallet_id > 0 AND destination_wallet_id IS NULL)
        OR (type = 'TRANSFER' AND source_wallet_id > 0 AND destination_wallet_id > 0
            AND source_wallet_id <> destination_wallet_id)
    ),
    CONSTRAINT required_wallet_sides CHECK (
        (type = 'DEPOSIT' AND destination_wallet_id IS NOT NULL)
        OR (type = 'WITHDRAWAL' AND source_wallet_id IS NOT NULL)
        OR (type = 'TRANSFER' AND source_wallet_id IS NOT NULL AND destination_wallet_id IS NOT NULL)
    )
);
