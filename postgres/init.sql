CREATE TABLE IF NOT EXISTS stock_data (
    symbol VARCHAR(10),
    timestamp TIMESTAMP,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume INTEGER
);