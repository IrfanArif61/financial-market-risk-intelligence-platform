import os
import sys
import pandas as pd

# Add project root to path
CURRENT_FILE = os.path.abspath(__file__)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(CURRENT_FILE)))
sys.path.append(PROJECT_ROOT)

from python.database.db_connection import get_engine  # noqa: E402


PROCESSED_DIR = os.path.join(PROJECT_ROOT, "data", "processed")
os.makedirs(PROCESSED_DIR, exist_ok=True)

OUTPUT_FILE_PATH = os.path.join(PROCESSED_DIR, "cleaned_market_data.csv")


def extract_raw_market_data(engine) -> pd.DataFrame:
    """
    Extract raw market price data joined with asset metadata from PostgreSQL.
    """
    query = """
        SELECT
            p.asset_id,
            a.ticker,
            a.asset_name,
            a.asset_class,
            a.exchange,
            a.currency,
            p.trade_date,
            p.open_price,
            p.high_price,
            p.low_price,
            p.close_price,
            p.adjusted_close,
            p.volume,
            p.data_source,
            p.loaded_at
        FROM market_risk.fact_market_prices p
        JOIN market_risk.dim_assets a
          ON p.asset_id = a.asset_id
        ORDER BY a.ticker, p.trade_date
    """

    df = pd.read_sql(query, engine)
    print(f"Extracted {len(df)} rows from PostgreSQL.")
    return df


def clean_market_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize market data for analytics.
    """
    if df.empty:
        raise ValueError("No data found in fact_market_prices.")

    print("\nStarting transformation and cleaning...")

    # Standardize date columns
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df["loaded_at"] = pd.to_datetime(df["loaded_at"], errors="coerce")

    # Convert numeric columns
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

    # Remove exact duplicate rows
    before_exact_dedup = len(df)
    df = df.drop_duplicates()
    exact_duplicates_removed = before_exact_dedup - len(df)

    # Remove duplicates on core business key
    before_business_dedup = len(df)
    df = df.drop_duplicates(subset=["asset_id", "trade_date"])
    business_duplicates_removed = before_business_dedup - len(df)

    # Remove rows with null critical fields
    before_null_drop = len(df)
    df = df.dropna(subset=["asset_id", "ticker", "trade_date", "close_price"])
    null_rows_removed = before_null_drop - len(df)

    # Remove invalid prices
    before_invalid_price_filter = len(df)
    df = df[
        (df["close_price"] > 0) &
        (df["open_price"].fillna(0) >= 0) &
        (df["high_price"].fillna(0) >= 0) &
        (df["low_price"].fillna(0) >= 0)
    ]
    invalid_price_rows_removed = before_invalid_price_filter - len(df)

    # Sort properly
    df = df.sort_values(by=["ticker", "trade_date"]).reset_index(drop=True)

    # Add row number per asset (helps debugging / analytics later)
    df["asset_row_num"] = df.groupby("ticker").cumcount() + 1

    # Add previous close placeholder (will be used in Stage 9)
    df["previous_close_price"] = df.groupby("ticker")["close_price"].shift(1)

    # Add data quality flags
    df["is_first_observation"] = df["previous_close_price"].isna()
    df["has_missing_adjusted_close"] = df["adjusted_close"].isna()
    df["has_missing_volume"] = df["volume"].isna()

    print("Transformation complete.")
    print(f"Exact duplicate rows removed: {exact_duplicates_removed}")
    print(f"Business-key duplicates removed: {business_duplicates_removed}")
    print(f"Null critical rows removed: {null_rows_removed}")
    print(f"Invalid price rows removed: {invalid_price_rows_removed}")
    print(f"Final cleaned rows: {len(df)}")

    return df


def validate_cleaned_data(df: pd.DataFrame):
    """
    Run validation checks on cleaned data.
    """
    print("\nRunning validation checks...")

    # Check duplicates
    duplicate_count = df.duplicated(subset=["asset_id", "trade_date"]).sum()
    print(f"Duplicate asset_id + trade_date rows: {duplicate_count}")

    # Check nulls in critical columns
    critical_nulls = df[["asset_id", "ticker", "trade_date", "close_price"]].isnull().sum()
    print("\nNull counts in critical columns:")
    print(critical_nulls)

    # Check row counts by ticker
    print("\nRow counts by ticker:")
    print(df.groupby("ticker").size().reset_index(name="row_count"))

    # Check date ranges by ticker
    print("\nDate ranges by ticker:")
    date_ranges = df.groupby("ticker").agg(
        min_date=("trade_date", "min"),
        max_date=("trade_date", "max")
    ).reset_index()
    print(date_ranges)

    # Check missing previous close count
    first_obs_count = df["is_first_observation"].sum()
    print(f"\nFirst observation rows (expected ~1 per ticker): {first_obs_count}")


def save_cleaned_data(df: pd.DataFrame, output_path: str):
    """
    Save cleaned data to processed folder.
    """
    df.to_csv(output_path, index=False)
    print(f"\nCleaned market data saved to:\n{output_path}")


def main():
    print("=" * 60)
    print("Stage 8 - Market Data Transformation Layer")
    print("=" * 60)

    engine = get_engine()

    raw_df = extract_raw_market_data(engine)
    cleaned_df = clean_market_data(raw_df)
    validate_cleaned_data(cleaned_df)
    save_cleaned_data(cleaned_df, OUTPUT_FILE_PATH)

    print("\nStage 8 completed successfully.")


if __name__ == "__main__":
    main()