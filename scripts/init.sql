CREATE TABLE IF NOT EXISTS averages (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(50),
    average FLOAT,
    calculated_at TIMESTAMPTZ
);