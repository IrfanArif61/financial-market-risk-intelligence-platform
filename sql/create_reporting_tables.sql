CREATE TABLE IF NOT EXISTS market_risk.fact_risk_metrics (
    risk_metric_id BIGSERIAL PRIMARY KEY,
    asset_id INT NOT NULL,
    trade_date DATE NOT NULL,
    daily_return NUMERIC(18,8),
    cumulative_return NUMERIC(18,8),
    volatility_7d NUMERIC(18,8),
    volatility_30d NUMERIC(18,8),
    var_95_30d NUMERIC(18,8),
    sharpe_ratio_30d NUMERIC(18,8),
    cumulative_wealth_index NUMERIC(18,8),
    running_peak NUMERIC(18,8),
    drawdown NUMERIC(18,8),
    max_drawdown_to_date NUMERIC(18,8),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_risk_asset
        FOREIGN KEY (asset_id)
        REFERENCES market_risk.dim_assets(asset_id),
    CONSTRAINT uq_risk_asset_trade_date
        UNIQUE (asset_id, trade_date)
);

CREATE TABLE IF NOT EXISTS market_risk.fact_portfolio_metrics (
    portfolio_metric_id BIGSERIAL PRIMARY KEY,
    trade_date DATE NOT NULL UNIQUE,
    portfolio_return NUMERIC(18,8),
    portfolio_cumulative_return NUMERIC(18,8),
    portfolio_volatility_30d NUMERIC(18,8),
    portfolio_var_95_30d NUMERIC(18,8),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS market_risk.fact_rolling_correlations (
    correlation_metric_id BIGSERIAL PRIMARY KEY,
    trade_date DATE NOT NULL,
    ticker_1 VARCHAR(20) NOT NULL,
    ticker_2 VARCHAR(20) NOT NULL,
    rolling_corr_30d NUMERIC(18,8),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_corr_trade_pair
        UNIQUE (trade_date, ticker_1, ticker_2)
);

CREATE INDEX IF NOT EXISTS idx_fact_risk_metrics_asset_id
    ON market_risk.fact_risk_metrics(asset_id);

CREATE INDEX IF NOT EXISTS idx_fact_risk_metrics_trade_date
    ON market_risk.fact_risk_metrics(trade_date);

CREATE INDEX IF NOT EXISTS idx_fact_portfolio_metrics_trade_date
    ON market_risk.fact_portfolio_metrics(trade_date);

CREATE INDEX IF NOT EXISTS idx_fact_rolling_correlations_trade_date
    ON market_risk.fact_rolling_correlations(trade_date);