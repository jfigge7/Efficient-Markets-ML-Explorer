from preprocessing_gpu_streaming import (
    build_observations_batched_stream,
    iter_comments_grouped_duckdb,
)

import os
from dotenv import load_dotenv
load_dotenv()

import duckdb
from pathlib import Path

con = duckdb.connect()
con.execute("PRAGMA threads=8")

COMMENTS = str(Path(os.getenv("DATA_DIR")) / "comment_db" / "year=2020" / "month=4" / "day=15" / "*.parquet")
SUBMISSIONS = str(Path(os.getenv("DATA_DIR")) / "submission_db" / "year=2020" / "month=4" / "day=15" / "*.parquet")

# submissions can still be loaded into memory (usually much smaller than comments)
submissions = con.execute(f"SELECT * FROM read_parquet('{SUBMISSIONS}')").df().to_dict("records")

# THIS is the streaming piece: an iterator of (submission_id, [comments...])
comment_groups = iter_comments_grouped_duckdb(con, COMMENTS, batch_rows=50_000)

paths = build_observations_batched_stream(
    comment_groups,
    submissions=submissions,
    rows_per_file=50_000,   # strongly recommended at scale
    file_format="parquet",
    file_prefix="observations",
)

print("Wrote:", len(paths), "files")