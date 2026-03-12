CREATE TABLE IF NOT EXISTS market_risk.dim_assets (
    asset_id SERIAL PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL UNIQUE,
    asset_name VARCHAR(100) NOT NULL,
    asset_class VARCHAR(50) NOT NULL,
    exchange VARCHAR(50),
    currency VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS market_risk.fact_market_prices (
    price_id BIGSERIAL PRIMARY KEY,
    asset_id INT NOT NULL,
    trade_date DATE NOT NULL,
    open_price NUMERIC(18,6),
    high_price NUMERIC(18,6),
    low_price NUMERIC(18,6),
    close_price NUMERIC(18,6),
    adjusted_close NUMERIC(18,6),
    volume NUMERIC(20,2),
    data_source VARCHAR(50),
    loaded_at TIMESTAMP,
    CONSTRAINT fk_asset
        FOREIGN KEY (asset_id)
        REFERENCES market_risk.dim_assets(asset_id),
    CONSTRAINT uq_asset_trade_date
        UNIQUE (asset_id, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_fact_market_prices_asset_id
    ON market_risk.fact_market_prices(asset_id);

CREATE INDEX IF NOT EXISTS idx_fact_market_prices_trade_date
    ON market_risk.fact_market_prices(trade_date);