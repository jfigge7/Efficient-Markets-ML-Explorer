import os
from dotenv import load_dotenv

import pandas as pd
from pathlib import Path

load_dotenv()

def create_parquet_files(env_src: str, root=Path(os.getenv("DATA_DIR")), chunksize=1000000):
    """
    Create parquet files for submissions/comments, batching every 1,000,000 rows of the csv.

    env_src: str
        The .env variable where the csv data is stored ("SUBMISSION_CSV" or "COMMENT_CSV")
    root: Path
        Path to the data directory
    chunksize: int
        Size of each batch (default 1,000,000)
    """
    for i, chunk in enumerate(pd.read_csv(os.getenv(env_src), parse_dates=["created_utc"], date_format=lambda x: pd.to_datetime(x, format="%Y-%m-%d %H:%M:%S", errors="coerce"), chunksize=chunksize)):
        chunk["created_utc"] = pd.to_datetime(chunk["created_utc"], utc=True, errors="coerce")
        chunk = chunk.dropna(subset=["created_utc"])
        chunk["year"] = chunk["created_utc"].dt.year.astype("int32")
        chunk["month"] = chunk["created_utc"].dt.month.astype("int8")

        root.mkdir(parents=True, exist_ok=True)
        # use a unique filename to avoid clashes when you re-run
        shard_path = root / f"{env_src.lower()}_part_{i:05d}.parquet"
        chunk.to_parquet(shard_path, index=False)

if __name__ == "__main__":
    pass