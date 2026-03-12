import os
import pandas as pd


# -----------------------------
# Project paths
# -----------------------------
CURRENT_FILE = os.path.abspath(__file__)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(CURRENT_FILE)))

INPUT_FILE_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "market_returns.csv")
OUTPUT_FILE_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "market_volatility.csv")


def load_returns_data(file_path: str) -> pd.DataFrame:
    """
    Load market returns dataset.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Input file not found: {file_path}")

    df = pd.read_csv(file_path)
    print(f"Loaded market returns data successfully. Rows found: {len(df)}")
    return df


def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare dataset for rolling volatility calculations.
    """
    required_columns = [
        "asset_id",
        "ticker",
        "trade_date",
        "daily_return",
    ]

    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df["daily_return"] = pd.to_numeric(df["daily_return"], errors="coerce")

    df = df.sort_values(by=["ticker", "trade_date"]).reset_index(drop=True)

    return df


def calculate_rolling_volatility(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate rolling volatility for each asset.
    """
    print("\nCalculating rolling volatility...")

    df["volatility_7d"] = (
        df.groupby("ticker")["daily_return"]
        .transform(lambda x: x.rolling(window=7, min_periods=7).std())
    )

    df["volatility_30d"] = (
        df.groupby("ticker")["daily_return"]
        .transform(lambda x: x.rolling(window=30, min_periods=30).std())
    )

    return df


def validate_volatility(df: pd.DataFrame):
    """
    Validate rolling volatility output.
    """
    print("\nRunning volatility validation checks...")

    print("\nNull counts:")
    print(df[["volatility_7d", "volatility_30d"]].isnull().sum())

    print("\nVolatility summary statistics:")
    print(df[["volatility_7d", "volatility_30d"]].describe())

    print("\nLatest volatility snapshot by ticker:")
    latest_vol = (
        df.sort_values(["ticker", "trade_date"])
          .groupby("ticker")
          .tail(1)[["ticker", "trade_date", "volatility_7d", "volatility_30d"]]
          .reset_index(drop=True)
    )
    print(latest_vol)

    print("\nTop 10 highest 30-day volatility observations:")
    top_vol = (
        df[["ticker", "trade_date", "volatility_30d"]]
        .dropna()
        .sort_values(by="volatility_30d", ascending=False)
        .head(10)
        .reset_index(drop=True)
    )
    print(top_vol)


def save_output(df: pd.DataFrame, file_path: str):
    """
    Save market volatility dataset.
    """
    df.to_csv(file_path, index=False)
    print(f"\nMarket volatility data saved to:\n{file_path}")


def main():
    print("=" * 60)
    print("Stage 9.2 - Rolling Volatility")
    print("=" * 60)

    df = load_returns_data(INPUT_FILE_PATH)
    df = prepare_data(df)
    df = calculate_rolling_volatility(df)
    validate_volatility(df)
    save_output(df, OUTPUT_FILE_PATH)

    print("\nStage 9.2 completed successfully.")


if __name__ == "__main__":
    main()