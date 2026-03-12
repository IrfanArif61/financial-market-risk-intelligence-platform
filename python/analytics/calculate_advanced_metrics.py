import os
import pandas as pd
import numpy as np
from itertools import combinations


# -----------------------------
# Paths
# -----------------------------
CURRENT_FILE = os.path.abspath(__file__)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(CURRENT_FILE)))

INPUT_FILE_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "market_risk_metrics.csv")
ADVANCED_OUTPUT_FILE_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "advanced_risk_metrics.csv")
CORRELATION_OUTPUT_FILE_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "rolling_correlations.csv")


def load_data(file_path: str) -> pd.DataFrame:
    """
    Load market risk metrics dataset.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Input file not found: {file_path}")

    df = pd.read_csv(file_path)
    print(f"Loaded market risk metrics dataset successfully. Rows found: {len(df)}")
    return df


def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare dataset for advanced metric calculations.
    """
    required_columns = [
        "asset_id",
        "ticker",
        "trade_date",
        "daily_return",
        "cumulative_return",
        "volatility_30d",
    ]

    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df["daily_return"] = pd.to_numeric(df["daily_return"], errors="coerce")
    df["cumulative_return"] = pd.to_numeric(df["cumulative_return"], errors="coerce")
    df["volatility_30d"] = pd.to_numeric(df["volatility_30d"], errors="coerce")

    df = df.sort_values(by=["ticker", "trade_date"]).reset_index(drop=True)

    return df


def calculate_sharpe_ratio(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate rolling 30-day Sharpe ratio using:
    rolling_mean(daily_return) / rolling_std(daily_return)

    Simplified version without risk-free rate for portfolio project.
    """
    print("\nCalculating 30-day rolling Sharpe ratio...")

    rolling_mean_return = (
        df.groupby("ticker")["daily_return"]
        .transform(lambda x: x.rolling(window=30, min_periods=30).mean())
    )

    df["sharpe_ratio_30d"] = np.where(
        df["volatility_30d"].notna() & (df["volatility_30d"] != 0),
        rolling_mean_return / df["volatility_30d"],
        np.nan
    )

    return df


def calculate_drawdown_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate running peak, drawdown, and max drawdown to date.
    """
    print("Calculating drawdown metrics...")

    # Convert cumulative return to cumulative wealth index
    df["cumulative_wealth_index"] = 1 + df["cumulative_return"]

    df["running_peak"] = (
        df.groupby("ticker")["cumulative_wealth_index"]
        .cummax()
    )

    df["drawdown"] = (
        df["cumulative_wealth_index"] / df["running_peak"]
    ) - 1

    df["max_drawdown_to_date"] = (
        df.groupby("ticker")["drawdown"]
        .cummin()
    )

    return df



def calculate_rolling_correlations(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate rolling 30-observation correlations for all ticker pairs.
    Uses only overlapping non-null dates for each pair.
    """
    print("Calculating rolling 30-day correlations...")

    pivot_df = df.pivot_table(
        index="trade_date",
        columns="ticker",
        values="daily_return"
    ).sort_index()

    correlation_rows = []

    ticker_pairs = list(combinations(pivot_df.columns, 2))

    for ticker_1, ticker_2 in ticker_pairs:
        pair_df = pivot_df[[ticker_1, ticker_2]].dropna().copy()

        if len(pair_df) < 30:
            continue

        pair_df["rolling_corr_30d"] = (
            pair_df[ticker_1]
            .rolling(window=30, min_periods=30)
            .corr(pair_df[ticker_2])
        )

        pair_df = pair_df.reset_index()
        pair_df["ticker_1"] = ticker_1
        pair_df["ticker_2"] = ticker_2

        pair_df = pair_df[["trade_date", "ticker_1", "ticker_2", "rolling_corr_30d"]]

        correlation_rows.append(pair_df)

    correlation_df = pd.concat(correlation_rows, ignore_index=True)

    return correlation_df


    
def validate_advanced_metrics(df: pd.DataFrame, corr_df: pd.DataFrame):
    """
    Validate advanced metrics output.
    """
    print("\nRunning advanced metrics validation...")

    print("\nNull counts in advanced asset metrics:")
    print(df[["sharpe_ratio_30d", "drawdown", "max_drawdown_to_date"]].isnull().sum())

    print("\nSharpe ratio summary:")
    print(df["sharpe_ratio_30d"].describe())

    print("\nWorst max drawdown by ticker:")
    worst_drawdowns = (
        df.groupby("ticker")["max_drawdown_to_date"]
        .min()
        .reset_index()
        .sort_values(by="max_drawdown_to_date")
    )
    print(worst_drawdowns)

    print("\nNull counts in rolling correlations:")
    print(corr_df["rolling_corr_30d"].isnull().sum())

    print("\nRolling correlation summary:")
    print(corr_df["rolling_corr_30d"].describe())

    print("\nLatest rolling correlation snapshot:")
    latest_corr = (
        corr_df.sort_values(["ticker_1", "ticker_2", "trade_date"])
        .groupby(["ticker_1", "ticker_2"])
        .tail(1)
        .reset_index(drop=True)
    )
    print(latest_corr.head(15))


def save_outputs(df: pd.DataFrame, corr_df: pd.DataFrame):
    """
    Save advanced metrics outputs.
    """
    df.to_csv(ADVANCED_OUTPUT_FILE_PATH, index=False)
    corr_df.to_csv(CORRELATION_OUTPUT_FILE_PATH, index=False)

    print(f"\nAdvanced asset metrics saved to:\n{ADVANCED_OUTPUT_FILE_PATH}")
    print(f"Rolling correlations saved to:\n{CORRELATION_OUTPUT_FILE_PATH}")


def main():


    df = load_data(INPUT_FILE_PATH)
    df = prepare_data(df)

    df = calculate_sharpe_ratio(df)
    df = calculate_drawdown_metrics(df)
    corr_df = calculate_rolling_correlations(df)

    validate_advanced_metrics(df, corr_df)
    save_outputs(df, corr_df)



if __name__ == "__main__":
    main()