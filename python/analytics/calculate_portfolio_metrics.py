import os
import pandas as pd


# -----------------------------
# Paths
# -----------------------------
CURRENT_FILE = os.path.abspath(__file__)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(CURRENT_FILE)))

INPUT_FILE_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "market_risk_metrics.csv")
OUTPUT_FILE_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "portfolio_risk_metrics.csv")


# Portfolio weights
PORTFOLIO_WEIGHTS = {
    "AAPL": 0.20,
    "TSLA": 0.15,
    "BTC-USD": 0.25,
    "ETH-USD": 0.15,
    "^GSPC": 0.15,
    "GLD": 0.10
}


def load_data(file_path):
    df = pd.read_csv(file_path)
    print(f"Loaded risk metrics dataset. Rows: {len(df)}")
    return df


def prepare_data(df):
    df["trade_date"] = pd.to_datetime(df["trade_date"])

    df = df[df["ticker"].isin(PORTFOLIO_WEIGHTS.keys())]

    return df


def calculate_portfolio_returns(df):

    print("\nCalculating portfolio returns...")

    df["weight"] = df["ticker"].map(PORTFOLIO_WEIGHTS)

    df["weighted_return"] = df["daily_return"] * df["weight"]

    portfolio = (
        df.groupby("trade_date")["weighted_return"]
        .sum()
        .reset_index()
    )

    portfolio.rename(columns={"weighted_return": "portfolio_return"}, inplace=True)

    portfolio = portfolio.sort_values("trade_date")

    portfolio["portfolio_cumulative_return"] = (
        (1 + portfolio["portfolio_return"])
        .cumprod() - 1
    )

    return portfolio


def calculate_portfolio_risk(portfolio):

    print("Calculating portfolio volatility and VaR...")

    portfolio["portfolio_volatility_30d"] = (
        portfolio["portfolio_return"]
        .rolling(window=30, min_periods=30)
        .std()
    )

    portfolio["portfolio_var_95_30d"] = (
        portfolio["portfolio_return"]
        .rolling(window=30, min_periods=30)
        .quantile(0.05)
    )

    return portfolio


def validate_results(portfolio):

    print("\nPortfolio summary:")

    print(portfolio.describe())

    print("\nLatest portfolio snapshot:")

    print(portfolio.tail(5))


def save_output(portfolio, file_path):

    portfolio.to_csv(file_path, index=False)

    print(f"\nPortfolio risk metrics saved to:\n{file_path}")


def main():

    print("=" * 60)
    print("Stage 9.4 - Portfolio Risk Analytics")
    print("=" * 60)

    df = load_data(INPUT_FILE_PATH)

    df = prepare_data(df)

    portfolio = calculate_portfolio_returns(df)

    portfolio = calculate_portfolio_risk(portfolio)

    validate_results(portfolio)

    save_output(portfolio, OUTPUT_FILE_PATH)

    print("\nStage 9.4 completed successfully.")


if __name__ == "__main__":
    main()