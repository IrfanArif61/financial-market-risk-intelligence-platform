INSERT INTO market_risk.dim_assets (ticker, asset_name, asset_class, exchange, currency)
VALUES
    ('AAPL', 'Apple Inc.', 'Equity', 'NASDAQ', 'USD'),
    ('TSLA', 'Tesla Inc.', 'Equity', 'NASDAQ', 'USD'),
    ('BTC-USD', 'Bitcoin', 'Crypto', 'Crypto', 'USD'),
    ('ETH-USD', 'Ethereum', 'Crypto', 'Crypto', 'USD'),
    ('^GSPC', 'S&P 500 Index', 'Index', 'S&P', 'USD'),
    ('GLD', 'SPDR Gold Shares', 'Commodity ETF', 'NYSE Arca', 'USD')
ON CONFLICT (ticker) DO NOTHING;