import os
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf


# -----------------------------
# Configuration
# -----------------------------
TICKERS = {
    "AAPL": {"asset_name": "Apple Inc.", "asset_class": "Equity", "exchange": "NASDAQ", "currency": "USD"},
    "TSLA": {"asset_name": "Tesla Inc.", "asset_class": "Equity", "exchange": "NASDAQ", "currency": "USD"},
    "BTC-USD": {"asset_name": "Bitcoin", "asset_class": "Crypto", "exchange": "Crypto", "currency": "USD"},
    "ETH-USD": {"asset_name": "Ethereum", "asset_class": "Crypto", "exchange": "Crypto", "currency": "USD"},
    "^GSPC": {"asset_name": "S&P 500 Index", "asset_class": "Index", "exchange": "S&P", "currency": "USD"},
    "GLD": {"asset_name": "SPDR Gold Shares", "asset_class": "Commodity ETF", "exchange": "NYSE Arca", "currency": "USD"},
}

DATA_SOURCE = "yfinance"
YEARS_BACK = 3

# Resolve project root dynamically
CURRENT_FILE = os.path.abspath(__file__)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(CURRENT_FILE)))

RAW_DATA_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
os.makedirs(RAW_DATA_DIR, exist_ok=True)


# -----------------------------
# Helper Functions
# -----------------------------
def get_date_range(years_back: int = 3):
    end_date = datetime.today().date()
    start_date = end_date - timedelta(days=365 * years_back)
    return start_date, end_date


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename columns into a consistent format.
    """
    df = df.rename(
        columns={
            "Date": "trade_date",
            "Open": "open_price",
            "High": "high_price",
            "Low": "low_price",
            "Close": "close_price",
            "Adj Close": "adjusted_close",
            "Volume": "volume",
        }
    )
    return df


def clean_downloaded_data(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """
    Clean raw yfinance data:
    - reset index
    - standardize columns
    - keep only needed columns
    - add metadata
    - remove null/duplicate rows
    """
    if df.empty:
        return df

    df = df.reset_index()

    # Flatten multi-index columns if they appear
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0] if isinstance(col, tuple) else col for col in df.columns]

    df = standardize_columns(df)

    # Some tickers may not return adjusted_close; handle safely
    expected_cols = [
        "trade_date",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "adjusted_close",
        "volume",
    ]

    for col in expected_cols:
        if col not in df.columns:
            df[col] = None

    df = df[expected_cols].copy()

    # Add metadata
    df["ticker"] = ticker
    df["asset_name"] = TICKERS[ticker]["asset_name"]
    df["asset_class"] = TICKERS[ticker]["asset_class"]
    df["exchange"] = TICKERS[ticker]["exchange"]
    df["currency"] = TICKERS[ticker]["currency"]
    df["data_source"] = DATA_SOURCE
    df["loaded_at"] = pd.Timestamp.now()

    # Data types
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    numeric_cols = [
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "adjusted_close",
        "volume",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Remove rows where critical prices are missing
    before_null_drop = len(df)
    df = df.dropna(subset=["trade_date", "close_price"])
    null_dropped = before_null_drop - len(df)

    # Remove duplicates by ticker + date
    before_dedup = len(df)
    df = df.drop_duplicates(subset=["ticker", "trade_date"])
    duplicates_removed = before_dedup - len(df)

    # Sort
    df = df.sort_values(by="trade_date").reset_index(drop=True)

    print(f"[{ticker}] Null rows removed: {null_dropped}")
    print(f"[{ticker}] Duplicate rows removed: {duplicates_removed}")
    print(f"[{ticker}] Final row count: {len(df)}")

    return df


def fetch_ticker_data(ticker: str, start_date, end_date) -> pd.DataFrame:
    """
    Fetch historical market data for one ticker.
    """
    print(f"\nFetching data for {ticker} from {start_date} to {end_date}...")

    try:
        df = yf.download(
            ticker,
            start=start_date,
            end=end_date,
            interval="1d",
            auto_adjust=False,
            progress=False,
        )
        if df.empty:
            print(f"[WARNING] No data returned for {ticker}")
            return pd.DataFrame()

        cleaned_df = clean_downloaded_data(df, ticker)
        return cleaned_df

    except Exception as e:
        print(f"[ERROR] Failed to fetch data for {ticker}: {e}")
        return pd.DataFrame()


def save_individual_csv(df: pd.DataFrame, ticker: str):
    """
    Save individual asset CSV file.
    """
    safe_ticker = ticker.replace("^", "")
    file_path = os.path.join(RAW_DATA_DIR, f"{safe_ticker}_raw_prices.csv")
    df.to_csv(file_path, index=False)
    print(f"[{ticker}] Saved individual CSV: {file_path}")


def save_combined_csv(df: pd.DataFrame):
    """
    Save combined raw dataset for all assets.
    """
    file_path = os.path.join(RAW_DATA_DIR, "combined_market_data_raw.csv")
    df.to_csv(file_path, index=False)
    print(f"\nSaved combined CSV: {file_path}")


# -----------------------------
# Main
# -----------------------------
def main():
    print("=" * 60)
    print("Financial Market Risk Intelligence Platform - Data Extraction")
    print("=" * 60)

    start_date, end_date = get_date_range(YEARS_BACK)
    all_dataframes = []

    for ticker in TICKERS:
        df = fetch_ticker_data(ticker, start_date, end_date)

        if not df.empty:
            save_individual_csv(df, ticker)
            all_dataframes.append(df)

    if not all_dataframes:
        print("\n[ERROR] No data was fetched for any ticker.")
        return

    combined_df = pd.concat(all_dataframes, ignore_index=True)
    combined_df = combined_df.sort_values(by=["ticker", "trade_date"]).reset_index(drop=True)

    save_combined_csv(combined_df)

    print("\nExtraction completed successfully.")
    print(f"Total rows in combined dataset: {len(combined_df)}")
    print("Tickers fetched:", combined_df["ticker"].nunique())
    print("Files saved in:", RAW_DATA_DIR)


if __name__ == "__main__":
    main()