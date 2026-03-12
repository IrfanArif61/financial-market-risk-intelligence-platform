import os
import pandas as pd
import numpy as np


# -----------------------------
# Project paths
# -----------------------------
CURRENT_FILE = os.path.abspath(__file__)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(CURRENT_FILE)))

INPUT_FILE_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "cleaned_market_data.csv")
OUTPUT_FILE_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "market_returns.csv")


def load_cleaned_data(file_path: str) -> pd.DataFrame:
    """
    Load cleaned market data from processed CSV.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Input file not found: {file_path}")

    df = pd.read_csv(file_path)
    print(f"Loaded cleaned market data successfully. Rows found: {len(df)}")
    return df


def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare and sort data before return calculations.
    """
    required_columns = [
        "asset_id",
        "ticker",
        "asset_name",
        "asset_class",
        "trade_date",
        "close_price",
        "previous_close_price",
        "is_first_observation",
    ]

    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df["close_price"] = pd.to_numeric(df["close_price"], errors="coerce")
    df["previous_close_price"] = pd.to_numeric(df["previous_close_price"], errors="coerce")

    df = df.sort_values(by=["ticker", "trade_date"]).reset_index(drop=True)

    return df


def calculate_daily_returns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate daily returns for each asset.
    """
    print("\nCalculating daily returns...")

    df["daily_return"] = np.where(
        df["previous_close_price"].notna() & (df["previous_close_price"] != 0),
        (df["close_price"] - df["previous_close_price"]) / df["previous_close_price"],
        np.nan
    )

    return df


def calculate_cumulative_returns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate cumulative returns for each asset.
    """
    print("Calculating cumulative returns...")

    df["daily_return_filled"] = df["daily_return"].fillna(0)

    df["cumulative_return"] = (
        df.groupby("ticker")["daily_return_filled"]
        .transform(lambda x: (1 + x).cumprod() - 1)
    )

    return df


def validate_returns(df: pd.DataFrame):
    """
    Validate calculated return outputs.
    """
    print("\nRunning return validation checks...")

    # Expect first row of each ticker to have null daily_return
    first_obs_daily_return_nulls = df[df["is_first_observation"] == True]["daily_return"].isna().sum()
    total_first_obs = (df["is_first_observation"] == True).sum()

    print(f"First observation rows: {total_first_obs}")
    print(f"First observation rows with NULL daily_return: {first_obs_daily_return_nulls}")

    # Null counts
    print("\nNull counts:")
    print(df[["daily_return", "cumulative_return"]].isnull().sum())

    # Basic summary stats
    print("\nDaily return summary:")
    print(df["daily_return"].describe())

    # Sample by ticker
    print("\nSample latest cumulative returns by ticker:")
    latest_returns = (
        df.sort_values(["ticker", "trade_date"])
          .groupby("ticker")
          .tail(1)[["ticker", "trade_date", "cumulative_return"]]
          .reset_index(drop=True)
    )
    print(latest_returns)


def save_output(df: pd.DataFrame, file_path: str):
    """
    Save returns dataset to processed folder.
    """
    # Drop helper column before saving
    if "daily_return_filled" in df.columns:
        df = df.drop(columns=["daily_return_filled"])

    df.to_csv(file_path, index=False)
    print(f"\nMarket returns data saved to:\n{file_path}")


def main():
    print("=" * 60)
    print("Stage 9.1 - Daily Returns and Cumulative Returns")
    print("=" * 60)

    df = load_cleaned_data(INPUT_FILE_PATH)
    df = prepare_data(df)
    df = calculate_daily_returns(df)
    df = calculate_cumulative_returns(df)
    validate_returns(df)
    save_output(df, OUTPUT_FILE_PATH)

    print("\nStage 9.1 completed successfully.")


if __name__ == "__main__":
    main()