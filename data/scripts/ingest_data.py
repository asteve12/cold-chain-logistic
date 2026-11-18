import os
import sys
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Load environment variables from .env file
load_dotenv()

# Project paths
BASE_DIR = Path(__file__).resolve().parent.parent.parent
CSV_FILE_PATH = BASE_DIR / "data" / "raw" / "dynamic_supply_chain_logistics_dataset.csv"
DEFAULT_TABLE_NAME = "supply_chain_logistics"


def normalize_render_db_url(db_url: str) -> str:
    """
    Normalizes a Render PostgreSQL URL for SQLAlchemy + psycopg2:
    1. Replaces 'postgres://' with 'postgresql+psycopg2://'
    2. Ensures 'sslmode=require' is set for external Render connections
    """
    db_url = db_url.strip()
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql+psycopg2://", 1)
    elif db_url.startswith("postgresql://"):
        db_url = db_url.replace("postgresql://", "postgresql+psycopg2://", 1)

    parsed = urlparse(db_url)
    query_params = dict(parse_qsl(parsed.query))

    # Render external URLs (ending in .render.com) require SSL
    if "render.com" in (parsed.hostname or "") and "sslmode" not in query_params:
        query_params["sslmode"] = "require"

    new_query = urlencode(query_params)
    return urlunparse(parsed._replace(query=new_query))


def ingest_csv_to_postgres(
    csv_path: Path = CSV_FILE_PATH,
    table_name: str = DEFAULT_TABLE_NAME,
    if_exists: str = "replace",
    chunksize: int = 2000,
) -> None:
    """
    Reads the supply chain logistics CSV dataset and dumps it into a Render PostgreSQL database.
    """
    raw_db_url = os.getenv("DATABASE_URL") or os.getenv("RENDER_DATABASE_URL")
    if not raw_db_url:
        print(
            "Error: DATABASE_URL is not set.\n"
            "Please add your Render External Database URL to a .env file in the project root:\n"
            "DATABASE_URL=postgresql://user:password@host.region-postgres.render.com/dbname"
        )
        sys.exit(1)

    if not csv_path.exists():
        print(f"Error: CSV file not found at {csv_path}")
        sys.exit(1)

    db_url = normalize_render_db_url(raw_db_url)

    print(f"Loading dataset from: {csv_path}")
    df = pd.read_csv(csv_path)

    # Clean column names and parse timestamp
    df.columns = [col.strip().lower() for col in df.columns]
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    total_rows, total_cols = df.shape
    print(f"Loaded {total_rows:,} rows and {total_cols} columns.")

    print("Connecting to Render PostgreSQL database...")
    engine = create_engine(db_url, pool_pre_ping=True)

    try:
        with engine.begin() as connection:
            print(f"Writing data to table '{table_name}' (if_exists='{if_exists}', chunksize={chunksize})...")
            df.to_sql(
                name=table_name,
                con=connection,
                if_exists=if_exists,
                index=False,
                chunksize=chunksize,
                method="multi",
            )

            # Verify row count in the database
            result = connection.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            db_count = result.scalar()

        print(f"Success! Dumped {db_count:,} rows into '{table_name}' on Render PostgreSQL.")
    except Exception as exc:
        print(f"Failed to ingest data into PostgreSQL: {exc}")
        sys.exit(1)
    finally:
        engine.dispose()


if __name__ == "__main__":
    table = os.getenv("DB_TABLE_NAME", DEFAULT_TABLE_NAME)
    ingest_csv_to_postgres(table_name=table)
