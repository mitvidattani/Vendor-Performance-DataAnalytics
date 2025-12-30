# ingestion_db.py
import logging
import time
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

# ---------------- LOGGING ----------------
Path("logs").mkdir(exist_ok=True)

logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a",
)

# ---------------- DATABASE ----------------
ENGINE = create_engine("sqlite:///inventory.db")

# ---------------- FUNCTIONS ----------------
def ingest_db(df: pd.DataFrame, table_name: str, engine=ENGINE):
    """
    Ingest dataframe into SQLite table
    """
    df.to_sql(table_name, con=engine, if_exists="replace", index=False)
    logging.info(f"Table `{table_name}` ingested successfully")


def load_raw_data(data_dir: Path = Path("data")):
    """
    Load all CSV files from data/ into SQLite
    """
    if not data_dir.exists():
        raise FileNotFoundError(f"Data directory not found: {data_dir}")

    start = time.time()

    for file in sorted(data_dir.glob("*.csv")):
        df = pd.read_csv(file)
        table_name = file.stem.lower()
        logging.info(f"Ingesting {file.name} -> {table_name}")
        ingest_db(df, table_name)

    mins = (time.time() - start) / 60
    logging.info(f"Ingestion completed in {mins:.2f} minutes")
    print(f"Done. Total ingestion time: {mins:.2f} minutes")


# ---------------- RUN ----------------
if __name__ == "__main__":
    load_raw_data()
