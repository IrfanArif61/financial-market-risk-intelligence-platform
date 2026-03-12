import os
import sys
import pandas as pd
from sqlalchemy import text

CURRENT_FILE = os.path.abspath(__file__)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(CURRENT_FILE)))
sys.path.append(PROJECT_ROOT)

from python.database.db_connection import get_engine  # noqa: E402

RAW_FILE_PATH = os.path.join(PROJECT_ROOT, "data", "raw", "combined_market_data_raw.csv")


def load_asset_mapping(engine):
    query = """
        SELECT asset_id, ticker
        FROM market_risk.dim_assets
    """
    df_assets = pd.read_sql(query, engine)
    return dict(zip(df_assets["ticker"], df_assets["asset_id"]))


def prepare_market_price_data(csv_path, asset_map):
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Raw CSV file not found: {csv_path}")

    df = pd.read_csv(csv_path)
    print(f"Raw CSV loaded successfully. Rows found: {len(df)}")

    required_columns = [
        "trade_date",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "adjusted_close",
        "volume",
        "ticker",
        "data_source",
        "loaded_at",
    ]

    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns in CSV: {missing_cols}")

    df = df[required_columns].copy()

    df["asset_id"] = df["ticker"].map(asset_map)

    missing_asset_ids = df[df["asset_id"].isna()]["ticker"].unique()
    if len(missing_asset_ids) > 0:
        raise ValueError(
            f"These tickers are missing in dim_assets and could not be mapped: {missing_asset_ids}"
        )

    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    df["loaded_at"] = pd.to_datetime(df["loaded_at"])

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

    before_dedup = len(df)
    df = df.drop_duplicates(subset=["asset_id", "trade_date"])
    duplicates_removed = before_dedup - len(df)

    print(f"Duplicate rows removed before DB load: {duplicates_removed}")
    print(f"Final rows prepared for insert: {len(df)}")

    df = df[
        [
            "asset_id",
            "trade_date",
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "adjusted_close",
            "volume",
            "data_source",
            "loaded_at",
        ]
    ].copy()

    return df


def remove_existing_asset_date_rows(engine, df):
    unique_pairs = df[["asset_id", "trade_date"]].drop_duplicates()

    with engine.begin() as conn:
        for _, row in unique_pairs.iterrows():
            conn.execute(
                text("""
                    DELETE FROM market_risk.fact_market_prices
                    WHERE asset_id = :asset_id
                      AND trade_date = :trade_date
                """),
                {"asset_id": int(row["asset_id"]), "trade_date": row["trade_date"]},
            )

    print("Existing matching asset-date rows removed from DB.")


def load_to_postgres(df, engine):
    df.to_sql(
        name="fact_market_prices",
        con=engine,
        schema="market_risk",
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )
    print("Data loaded successfully into market_risk.fact_market_prices")


def validate_load(engine):
    queries = {
        "Total rows in fact_market_prices": """
            SELECT COUNT(*) AS total_rows
            FROM market_risk.fact_market_prices
        """,
        "Rows by ticker": """
            SELECT a.ticker, COUNT(*) AS row_count
            FROM market_risk.fact_market_prices p
            JOIN market_risk.dim_assets a
              ON p.asset_id = a.asset_id
            GROUP BY a.ticker
            ORDER BY a.ticker
        """,
        "Date range by ticker": """
            SELECT a.ticker,
                   MIN(p.trade_date) AS min_date,
                   MAX(p.trade_date) AS max_date
            FROM market_risk.fact_market_prices p
            JOIN market_risk.dim_assets a
              ON p.asset_id = a.asset_id
            GROUP BY a.ticker
            ORDER BY a.ticker
        """
    }

    for title, query in queries.items():
        print(f"\n--- {title} ---")
        result_df = pd.read_sql(query, engine)
        print(result_df)


def main():
    print("=" * 60)
    print("Loading raw market data into Neon PostgreSQL")
    print("=" * 60)

    engine = get_engine()

    asset_map = load_asset_mapping(engine)
    print(f"Loaded asset mapping for {len(asset_map)} assets.")

    df_prices = prepare_market_price_data(RAW_FILE_PATH, asset_map)

    remove_existing_asset_date_rows(engine, df_prices)
    load_to_postgres(df_prices, engine)
    validate_load(engine)

    print("\nStage 7 completed successfully.")


if __name__ == "__main__":
    main()