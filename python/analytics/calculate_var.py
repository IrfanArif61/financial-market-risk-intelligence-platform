import os
import pandas as pd


# -----------------------------
# Project paths
# -----------------------------
CURRENT_FILE = os.path.abspath(__file__)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(CURRENT_FILE)))

INPUT_FILE_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "market_volatility.csv")
OUTPUT_FILE_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "market_risk_metrics.csv")


def load_volatility_data(file_path: str) -> pd.DataFrame:
    """
    Load dataset containing returns and volatility.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Input file not found: {file_path}")

    df = pd.read_csv(file_path)
    print(f"Loaded volatility dataset successfully. Rows found: {len(df)}")
    return df


def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare dataset for VaR calculation.
    """
    required_columns = [
        "ticker",
        "trade_date",
        "daily_return"
    ]

    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df["daily_return"] = pd.to_numeric(df["daily_return"], errors="coerce")

    df = df.sort_values(by=["ticker", "trade_date"]).reset_index(drop=True)

    return df


def calculate_var(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate rolling 30-day historical VaR (95%).
    """
    print("\nCalculating 30-day Value at Risk (95%)...")

    df["var_95_30d"] = (
        df.groupby("ticker")["daily_return"]
        .transform(lambda x: x.rolling(window=30, min_periods=30)
        .quantile(0.05))
    )

    return df


def validate_var(df: pd.DataFrame):
    """
    Validate VaR output.
    """
    print("\nRunning VaR validation checks...")

    print("\nNull counts:")
    print(df["var_95_30d"].isnull().sum())

    print("\nVaR summary statistics:")
    print(df["var_95_30d"].describe())

    print("\nLatest VaR snapshot by ticker:")
    latest_var = (
        df.sort_values(["ticker", "trade_date"])
        .groupby("ticker")
        .tail(1)[["ticker", "trade_date", "var_95_30d"]]
        .reset_index(drop=True)
    )

    print(latest_var)

    print("\nWorst 10 VaR observations:")
    worst_var = (
        df[["ticker", "trade_date", "var_95_30d"]]
        .dropna()
        .sort_values(by="var_95_30d")
        .head(10)
    )

    print(worst_var)


def save_output(df: pd.DataFrame, file_path: str):
    """
    Save risk metrics dataset.
    """
    df.to_csv(file_path, index=False)

    print(f"\nMarket risk metrics saved to:\n{file_path}")


def main():

    

    df = load_volatility_data(INPUT_FILE_PATH)
    df = prepare_data(df)

    df = calculate_var(df)

    validate_var(df)

    save_output(df, OUTPUT_FILE_PATH)



if __name__ == "__main__":
    main()