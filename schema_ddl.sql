-- ==========================================
-- 1. DIMENSION TABLES (3NF Normalization)
-- ==========================================

-- Exchanges Table: Stores unique exchanges to avoid string repetition
CREATE TABLE Exchanges (
    exchange_id SERIAL PRIMARY KEY,
    exchange_code VARCHAR(10) UNIQUE NOT NULL 
);

-- Instruments Table: Stores underlying assets
CREATE TABLE Instruments (
    instrument_id SERIAL PRIMARY KEY,
    exchange_id INT REFERENCES Exchanges(exchange_id),
    symbol VARCHAR(50) NOT NULL,
    instrument_type VARCHAR(20) NOT NULL, 
    UNIQUE (exchange_id, symbol, instrument_type)
);

-- Expiries (Contracts) Table: Separates strike and expiry details
CREATE TABLE Expiries (
    contract_id SERIAL PRIMARY KEY,
    instrument_id INT REFERENCES Instruments(instrument_id),
    expiry_dt DATE NOT NULL,
    strike_pr NUMERIC(15, 4), 
    option_typ VARCHAR(2),    
    UNIQUE (instrument_id, expiry_dt, strike_pr, option_typ)
);

-- ==========================================
-- 2. FACT TABLE & PARTITIONING
-- ==========================================

-- Trades Table: Partitioned by date for time-series optimization
CREATE TABLE Trades (
    trade_id BIGSERIAL,
    contract_id INT REFERENCES Expiries(contract_id),
    trade_date DATE NOT NULL, 
    open_pr NUMERIC(15, 4),
    high_pr NUMERIC(15, 4),
    low_pr NUMERIC(15, 4),
    close_pr NUMERIC(15, 4),
    settle_pr NUMERIC(15, 4),
    volume BIGINT,
    open_int BIGINT,
    recorded_at TIMESTAMP NOT NULL,
    PRIMARY KEY (trade_date, trade_id) 
) PARTITION BY RANGE (trade_date);

-- Creating Partitions for the 3 months of data
CREATE TABLE trades_m1 PARTITION OF Trades FOR VALUES FROM ('2023-01-01') TO ('2023-02-01');
CREATE TABLE trades_m2 PARTITION OF Trades FOR VALUES FROM ('2023-02-01') TO ('2023-03-01');
CREATE TABLE trades_m3 PARTITION OF Trades FOR VALUES FROM ('2023-03-01') TO ('2023-04-01');

-- ==========================================
-- 3. INDEXING FOR PERFORMANCE
-- ==========================================

-- BRIN index for massive, sequentially ingested time-series data
CREATE INDEX idx_trades_recorded_at ON Trades USING BRIN (recorded_at);

-- B-Tree indexes for frequent filtering and foreign key joins
CREATE INDEX idx_trades_contract_id ON Trades (contract_id);
CREATE INDEX idx_instruments_symbol ON Instruments (symbol);
CREATE INDEX idx_expiries_expiry_dt ON Expiries (expiry_dt);
