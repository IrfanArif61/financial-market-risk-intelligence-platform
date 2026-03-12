# Data Dictionary

Complete documentation of all database tables, columns, and views used in the Financial Market Risk Intelligence Platform.

---

## Schema: `market_risk`

---

### `dim_assets` — Asset Dimension Table

Stores metadata for each financial asset tracked by the platform.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `asset_id` | SERIAL (PK) | No | Auto-generated unique asset identifier |
| `ticker` | VARCHAR(20) | No | Market ticker symbol (e.g., AAPL, BTC-USD) |
| `asset_name` | VARCHAR(100) | No | Full asset name (e.g., Apple Inc.) |
| `asset_class` | VARCHAR(50) | No | Asset classification (Equity, Crypto, Index, Commodity ETF) |
| `exchange` | VARCHAR(50) | Yes | Exchange where the asset is traded |
| `currency` | VARCHAR(10) | Yes | Trading currency (e.g., USD) |
| `created_at` | TIMESTAMP | No | Record creation timestamp (auto-set) |

**Unique Constraint:** `ticker`

---

### `fact_market_prices` — Daily Price Fact Table

Stores daily OHLCV (Open, High, Low, Close, Volume) market data for each asset.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `price_id` | BIGSERIAL (PK) | No | Auto-generated unique price record ID |
| `asset_id` | INT (FK) | No | References `dim_assets.asset_id` |
| `trade_date` | DATE | No | Trading date |
| `open_price` | NUMERIC(18,6) | Yes | Opening price |
| `high_price` | NUMERIC(18,6) | Yes | Highest price during the day |
| `low_price` | NUMERIC(18,6) | Yes | Lowest price during the day |
| `close_price` | NUMERIC(18,6) | Yes | Closing price |
| `adjusted_close` | NUMERIC(18,6) | Yes | Adjusted closing price (splits/dividends) |
| `volume` | NUMERIC(20,2) | Yes | Trading volume |
| `data_source` | VARCHAR(50) | Yes | Data provider (e.g., yfinance) |
| `loaded_at` | TIMESTAMP | Yes | Timestamp when record was loaded |

**Foreign Key:** `asset_id` → `dim_assets.asset_id`
**Unique Constraint:** `(asset_id, trade_date)`
**Indexes:** `asset_id`, `trade_date`

---

### `fact_risk_metrics` — Asset Risk Metrics Fact Table

Stores calculated daily risk metrics per asset.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `risk_metric_id` | BIGSERIAL (PK) | No | Auto-generated unique ID |
| `asset_id` | INT (FK) | No | References `dim_assets.asset_id` |
| `trade_date` | DATE | No | Calculation date |
| `daily_return` | NUMERIC(18,8) | Yes | Daily percentage return |
| `cumulative_return` | NUMERIC(18,8) | Yes | Compounded return since start |
| `volatility_7d` | NUMERIC(18,8) | Yes | 7-day rolling standard deviation of returns |
| `volatility_30d` | NUMERIC(18,8) | Yes | 30-day rolling standard deviation of returns |
| `var_95_30d` | NUMERIC(18,8) | Yes | 30-day rolling VaR at 95% confidence (5th percentile) |
| `sharpe_ratio_30d` | NUMERIC(18,8) | Yes | 30-day rolling Sharpe ratio (mean/std) |
| `cumulative_wealth_index` | NUMERIC(18,8) | Yes | Cumulative wealth index (1 + cumulative return) |
| `running_peak` | NUMERIC(18,8) | Yes | Running maximum of cumulative wealth index |
| `drawdown` | NUMERIC(18,8) | Yes | Current drawdown from running peak |
| `max_drawdown_to_date` | NUMERIC(18,8) | Yes | Worst drawdown observed to date |
| `loaded_at` | TIMESTAMP | No | Load timestamp (auto-set) |

**Foreign Key:** `asset_id` → `dim_assets.asset_id`
**Unique Constraint:** `(asset_id, trade_date)`
**Indexes:** `asset_id`, `trade_date`

---

### `fact_portfolio_metrics` — Portfolio Risk Metrics Fact Table

Stores daily risk metrics at the portfolio level (weighted across all assets).

| Column | Type | Nullable | Description |
|---|---|---|---|
| `portfolio_metric_id` | BIGSERIAL (PK) | No | Auto-generated unique ID |
| `trade_date` | DATE | No | Calculation date |
| `portfolio_return` | NUMERIC(18,8) | Yes | Weighted sum of asset daily returns |
| `portfolio_cumulative_return` | NUMERIC(18,8) | Yes | Compounded portfolio return |
| `portfolio_volatility_30d` | NUMERIC(18,8) | Yes | 30-day rolling portfolio volatility |
| `portfolio_var_95_30d` | NUMERIC(18,8) | Yes | 30-day rolling portfolio VaR (95%) |
| `loaded_at` | TIMESTAMP | No | Load timestamp (auto-set) |

**Unique Constraint:** `trade_date`
**Index:** `trade_date`

---

### `fact_rolling_correlations` — Asset Correlation Fact Table

Stores 30-day rolling pairwise correlations between all assets.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `correlation_metric_id` | BIGSERIAL (PK) | No | Auto-generated unique ID |
| `trade_date` | DATE | No | Calculation date |
| `ticker_1` | VARCHAR(20) | No | First asset in the pair |
| `ticker_2` | VARCHAR(20) | No | Second asset in the pair |
| `rolling_corr_30d` | NUMERIC(18,8) | Yes | 30-day rolling Pearson correlation |
| `loaded_at` | TIMESTAMP | No | Load timestamp (auto-set) |

**Unique Constraint:** `(trade_date, ticker_1, ticker_2)`
**Index:** `trade_date`

---

## Views (Semantic Layer)

### `v_asset_risk_metrics`

Joins `fact_risk_metrics` with `dim_assets` to provide enriched asset risk data with metadata (ticker, name, class).

### `v_portfolio_risk_metrics`

Clean passthrough of `fact_portfolio_metrics` for BI consumption.

### `v_asset_correlations`

Clean passthrough of `fact_rolling_correlations` for correlation matrix visualization.

### `v_latest_asset_risk_metrics`

Returns only the most recent risk metrics per asset — used for the "Latest Market Snapshot" table in Power BI.
