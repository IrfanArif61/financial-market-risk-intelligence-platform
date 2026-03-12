import os
import sys
import pandas as pd
from sqlalchemy import text

# Add project root to path
CURRENT_FILE = os.path.abspath(__file__)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(CURRENT_FILE)))
sys.path.append(PROJECT_ROOT)

from python.database.db_connection import get_engine  # noqa: E402


ADVANCED_FILE_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "advanced_risk_metrics.csv")
PORTFOLIO_FILE_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "portfolio_risk_metrics.csv")
CORRELATION_FILE_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "rolling_correlations.csv")


def load_csv(file_path: str, name: str) -> pd.DataFrame:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"{name} file not found: {file_path}")

    df = pd.read_csv(file_path)
    print(f"{name} loaded successfully. Rows found: {len(df)}")
    return df


def prepare_risk_metrics(df: pd.DataFrame) -> pd.DataFrame:
    required_columns = [
        "asset_id",
        "trade_date",
        "daily_return",
        "cumulative_return",
        "volatility_7d",
        "volatility_30d",
        "var_95_30d",
        "sharpe_ratio_30d",
        "cumulative_wealth_index",
        "running_peak",
        "drawdown",
        "max_drawdown_to_date",
    ]

    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing columns in advanced_risk_metrics.csv: {missing_cols}")

    df = df[required_columns].copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date

    numeric_cols = [col for col in required_columns if col not in ["asset_id", "trade_date"]]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["asset_id"] = pd.to_numeric(df["asset_id"], errors="coerce").astype("Int64")
    df = df.drop_duplicates(subset=["asset_id", "trade_date"])

    return df


def prepare_portfolio_metrics(df: pd.DataFrame) -> pd.DataFrame:
    required_columns = [
        "trade_date",
        "portfolio_return",
        "portfolio_cumulative_return",
        "portfolio_volatility_30d",
        "portfolio_var_95_30d",
    ]

    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing columns in portfolio_risk_metrics.csv: {missing_cols}")

    df = df[required_columns].copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date

    for col in required_columns:
        if col != "trade_date":
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.drop_duplicates(subset=["trade_date"])

    return df


def prepare_correlations(df: pd.DataFrame) -> pd.DataFrame:
    required_columns = [
        "trade_date",
        "ticker_1",
        "ticker_2",
        "rolling_corr_30d",
    ]

    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing columns in rolling_correlations.csv: {missing_cols}")

    df = df[required_columns].copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    df["rolling_corr_30d"] = pd.to_numeric(df["rolling_corr_30d"], errors="coerce")

    df["ticker_1"] = df["ticker_1"].astype(str)
    df["ticker_2"] = df["ticker_2"].astype(str)

    df = df.drop_duplicates(subset=["trade_date", "ticker_1", "ticker_2"])

    return df


def delete_existing_risk_metrics(engine, df: pd.DataFrame):
    with engine.begin() as conn:
        for _, row in df[["asset_id", "trade_date"]].drop_duplicates().iterrows():
            conn.execute(
                text("""
                    DELETE FROM market_risk.fact_risk_metrics
                    WHERE asset_id = :asset_id
                      AND trade_date = :trade_date
                """),
                {"asset_id": int(row["asset_id"]), "trade_date": row["trade_date"]},
            )
    print("Existing matching rows removed from fact_risk_metrics.")


def delete_existing_portfolio_metrics(engine, df: pd.DataFrame):
    with engine.begin() as conn:
        for _, row in df[["trade_date"]].drop_duplicates().iterrows():
            conn.execute(
                text("""
                    DELETE FROM market_risk.fact_portfolio_metrics
                    WHERE trade_date = :trade_date
                """),
                {"trade_date": row["trade_date"]},
            )
    print("Existing matching rows removed from fact_portfolio_metrics.")


def delete_existing_correlations(engine, df: pd.DataFrame):
    with engine.begin() as conn:
        for _, row in df[["trade_date", "ticker_1", "ticker_2"]].drop_duplicates().iterrows():
            conn.execute(
                text("""
                    DELETE FROM market_risk.fact_rolling_correlations
                    WHERE trade_date = :trade_date
                      AND ticker_1 = :ticker_1
                      AND ticker_2 = :ticker_2
                """),
                {
                    "trade_date": row["trade_date"],
                    "ticker_1": row["ticker_1"],
                    "ticker_2": row["ticker_2"],
                },
            )
    print("Existing matching rows removed from fact_rolling_correlations.")


def load_table(df: pd.DataFrame, engine, table_name: str):
    df.to_sql(
        name=table_name,
        con=engine,
        schema="market_risk",
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )
    print(f"Data loaded successfully into market_risk.{table_name}")


def validate_load(engine):
    queries = {
        "fact_risk_metrics row count": """
            SELECT COUNT(*) AS total_rows
            FROM market_risk.fact_risk_metrics
        """,
        "fact_portfolio_metrics row count": """
            SELECT COUNT(*) AS total_rows
            FROM market_risk.fact_portfolio_metrics
        """,
        "fact_rolling_correlations row count": """
            SELECT COUNT(*) AS total_rows
            FROM market_risk.fact_rolling_correlations
        """,
    }

    for title, query in queries.items():
        print(f"\n--- {title} ---")
        print(pd.read_sql(query, engine))


def main():
   

    engine = get_engine()

    # Load CSVs
    advanced_df = load_csv(ADVANCED_FILE_PATH, "Advanced risk metrics")
    portfolio_df = load_csv(PORTFOLIO_FILE_PATH, "Portfolio risk metrics")
    correlation_df = load_csv(CORRELATION_FILE_PATH, "Rolling correlations")

    # Prepare data
    risk_metrics_df = prepare_risk_metrics(advanced_df)
    portfolio_metrics_df = prepare_portfolio_metrics(portfolio_df)
    correlations_df = prepare_correlations(correlation_df)

    print(f"\nPrepared fact_risk_metrics rows: {len(risk_metrics_df)}")
    print(f"Prepared fact_portfolio_metrics rows: {len(portfolio_metrics_df)}")
    print(f"Prepared fact_rolling_correlations rows: {len(correlations_df)}")

    # Delete existing matching rows
    delete_existing_risk_metrics(engine, risk_metrics_df)
    delete_existing_portfolio_metrics(engine, portfolio_metrics_df)
    delete_existing_correlations(engine, correlations_df)

    # Load tables
    load_table(risk_metrics_df, engine, "fact_risk_metrics")
    load_table(portfolio_metrics_df, engine, "fact_portfolio_metrics")
    load_table(correlations_df, engine, "fact_rolling_correlations")

    # Validate
    validate_load(engine)



if __name__ == "__main__":
    main()