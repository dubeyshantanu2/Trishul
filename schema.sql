-- Enable UUID extension if not present
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Signal Audit Logs Table
CREATE TABLE IF NOT EXISTS trishul_signals (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc'::text, NOW()) NOT NULL,
    timestamp_epoch BIGINT NOT NULL,
    instrument_ticker VARCHAR(50) NOT NULL,
    signal_direction VARCHAR(10) NOT NULL, -- 'LONG' or 'SHORT'
    spoof_filtered_obi NUMERIC(5,2) NOT NULL,
    current_cvd BIGINT NOT NULL,
    avp_poc NUMERIC(10,2) NOT NULL,
    avp_vah NUMERIC(10,2) NOT NULL,
    avp_val NUMERIC(10,2) NOT NULL,
    calculated_micro_price NUMERIC(10,2) NOT NULL,
    calculated_sweep_vwap NUMERIC(10,2) NOT NULL,
    suggested_atm_strike VARCHAR(20) NOT NULL,
    current_regime VARCHAR(20) NOT NULL -- 'TRENDING' or 'CHOP'
);

-- Performance Indexes for Quick Post-Trade Analysis
CREATE INDEX IF NOT EXISTS idx_trishul_ticker ON trishul_signals(instrument_ticker);
CREATE INDEX IF NOT EXISTS idx_trishul_timestamp ON trishul_signals(timestamp_epoch DESC);
