-- =================================================================
-- Semantic Views — BI Consumption Layer
-- These views provide clean, enriched data for Power BI dashboards
-- =================================================================

-- View 1: Asset Risk Metrics (enriched with asset metadata)
CREATE OR REPLACE VIEW market_risk.v_asset_risk_metrics AS
SELECT
    a.asset_id,
    a.ticker,
    a.asset_name,
    a.asset_class,
    a.exchange,
    a.currency,
    r.trade_date,
    r.daily_return,
    r.cumulative_return,
    r.volatility_7d,
    r.volatility_30d,
    r.var_95_30d,
    r.sharpe_ratio_30d,
    r.cumulative_wealth_index,
    r.drawdown,
    r.max_drawdown_to_date
FROM market_risk.fact_risk_metrics r
JOIN market_risk.dim_assets a
  ON r.asset_id = a.asset_id;


-- View 2: Portfolio Risk Metrics
CREATE OR REPLACE VIEW market_risk.v_portfolio_risk_metrics AS
SELECT
    trade_date,
    portfolio_return,
    portfolio_cumulative_return,
    portfolio_volatility_30d,
    portfolio_var_95_30d
FROM market_risk.fact_portfolio_metrics;


-- View 3: Asset Correlations
CREATE OR REPLACE VIEW market_risk.v_asset_correlations AS
SELECT
    trade_date,
    ticker_1,
    ticker_2,
    rolling_corr_30d
FROM market_risk.fact_rolling_correlations;


-- View 4: Latest Asset Risk Snapshot (most recent metrics per asset)
CREATE OR REPLACE VIEW market_risk.v_latest_asset_risk_metrics AS
SELECT DISTINCT ON (a.asset_id)
    a.asset_id,
    a.ticker,
    a.asset_name,
    a.asset_class,
    r.trade_date,
    r.daily_return,
    r.cumulative_return,
    r.volatility_30d,
    r.var_95_30d,
    r.sharpe_ratio_30d,
    r.drawdown,
    r.max_drawdown_to_date
FROM market_risk.fact_risk_metrics r
JOIN market_risk.dim_assets a
  ON r.asset_id = a.asset_id
ORDER BY a.asset_id, r.trade_date DESC;


-- View 5: Asset Performance Summary (aggregated stats per asset)
CREATE OR REPLACE VIEW market_risk.v_asset_performance_summary AS
SELECT
    a.asset_id,
    a.ticker,
    a.asset_name,
    a.asset_class,
    MAX(r.trade_date) AS latest_date,
    COUNT(*) AS total_observations,
    MAX(r.cumulative_return) AS max_cumulative_return,
    MIN(r.drawdown) AS worst_drawdown,
    MIN(r.max_drawdown_to_date) AS max_drawdown_to_date,
    AVG(r.daily_return) AS avg_daily_return,
    AVG(r.volatility_30d) AS avg_volatility_30d,
    AVG(r.sharpe_ratio_30d) AS avg_sharpe_ratio_30d
FROM market_risk.fact_risk_metrics r
JOIN market_risk.dim_assets a
  ON r.asset_id = a.asset_id
GROUP BY
    a.asset_id,
    a.ticker,
    a.asset_name,
    a.asset_class;
