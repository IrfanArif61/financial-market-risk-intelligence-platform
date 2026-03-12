import os
import sys
import pandas as pd

CURRENT_FILE = os.path.abspath(__file__)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(CURRENT_FILE)))
sys.path.append(PROJECT_ROOT)

from python.database.db_connection import get_engine  # noqa: E402


def main():
    engine = get_engine()
    query = "SELECT version();"
    df = pd.read_sql(query, engine)
    print(df)


if __name__ == "__main__":
    main()