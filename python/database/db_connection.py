import os
from dotenv import load_dotenv
from sqlalchemy import create_engine


load_dotenv()


def get_engine():
    """
    Returns SQLAlchemy engine for Neon PostgreSQL connection.
    Reads credentials from .env file.
    """
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT", "5432")
    database = os.getenv("DB_NAME")
    username = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    sslmode = os.getenv("DB_SSLMODE", "require")

    if not all([host, port, database, username, password]):
        raise ValueError("Missing one or more database environment variables in .env")

    connection_string = (
        f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"
        f"?sslmode={sslmode}"
    )

    engine = create_engine(connection_string)
    return engine