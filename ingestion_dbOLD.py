#INGESTION SCRIPT TO LOAD RAW DATA INTO SQLITE DATABASE

import sys
import subprocess
import logging
import time
from pathlib import Path

# 1) Ensure pip exists, then install SQLAlchemy into this kernel
subprocess.check_call([sys.executable, "-m", "ensurepip", "--upgrade"])
subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "sqlalchemy"])

# 2) Imports (after installation)
import pandas as pd
from sqlalchemy import create_engine

# 3) Logging setup
Path("logs").mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a",
)

# 4) Database engine
engine = create_engine("sqlite:///inventory.db")
print(engine)

# 5) Ingestion Functions
def ingest_db(df, table_name, engine):
    df.to_sql(table_name, con=engine, if_exists="replace", index=False)

def load_raw_data(data_dir: Path = Path("data")):
    """Ingest all CSV files from data/ into the SQLite database."""
    if not data_dir.exists():
        raise FileNotFoundError(f"Couldn't find data directory at: {data_dir.resolve()}")

    start = time.time()
    for file in sorted(data_dir.iterdir()):
        if file.is_file() and file.suffix.lower() == ".csv":
            df = pd.read_csv(file)
            logging.info(f"Ingesting {file} into db")
            ingest_db(df, file.stem, engine)
    end = time.time()
    total_time_min = (end - start) / 60
    logging.info("Ingestion Complete")
    logging.info(f"Total Ingestion time: {total_time_min} minutes")
    return total_time_min

# 6) Run ingestion
# minutes = load_raw_data()
# print(f"Done. Total ingestion time: {minutes:.2f} minutes")

# 6) Run ingestion
def main():
    minutes = load_raw_data()
    print(f"Done. Total ingestion time: {minutes:.2f} minutes")


if __name__ == "__main__":
    main()
