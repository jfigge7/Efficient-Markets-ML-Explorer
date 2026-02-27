from preprocessing_gpu_streaming import (
    build_observations_batched_stream,
    iter_comments_grouped_duckdb,
)

import os
from dotenv import load_dotenv
load_dotenv()

import duckdb
from pathlib import Path
from datetime import date, timedelta
from typing import Dict, Any, Iterator, Tuple, List, Iterable, Set

DATA_DIR = Path(os.getenv("DATA_DIR")).expanduser().resolve()

def day_glob(base: Path, d: date) -> str:
    return str(
        base
        / f"year={d.year}"
        / f"month={d.month}"
        / f"day={d.day}"
        / "*.parquet"
    )

def iter_days(start: date, end: date) -> Iterator[date]:
    # inclusive start, inclusive end
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)

def build_sub_index(submissions: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    # same behavior as your _index_submissions, but local to driver
    idx: Dict[str, Dict[str, Any]] = {}
    for s in submissions:
        sid = str(s.get("id", "")).strip()
        if not sid:
            continue
        if sid.startswith("t3_"):
            idx[sid[3:]] = s
            idx[sid] = s
        else:
            idx[sid] = s
            idx[f"t3_{sid}"] = s
    return idx

def filter_groups_to_submission_set(
    groups: Iterable[Tuple[str, List[Dict[str, Any]]]],
    allowed_submission_ids: Set[str],
) -> Iterator[Tuple[str, List[Dict[str, Any]]]]:
    """
    groups yields (submission_id_base, [comment_dicts]) from comment stream.
    We only pass through groups whose submission_id is in today's submissions.
    """
    for sid, comments in groups:
        if sid in allowed_submission_ids or f"t3_{sid}" in allowed_submission_ids:
            yield sid, comments

# ----------------------------
# Main driver (day-by-day)
# ----------------------------
con = duckdb.connect()
con.execute("PRAGMA threads=8")

SUB_BASE = DATA_DIR / "submission_db"
COM_BASE = DATA_DIR / "comment_db"

# Pick your range (example)
START_DAY = date(2020, 4, 10)
END_DAY   = date(2020, 4, 12)

OUT_DIR = Path(os.getenv("OBS_OUTPUT_DIR")).expanduser().resolve()  # or wherever you want

for sub_day in iter_days(START_DAY, END_DAY):
    next_day = sub_day + timedelta(days=1)

    submissions_glob = day_glob(SUB_BASE, sub_day)
    comments_glob_today = day_glob(COM_BASE, sub_day)
    comments_glob_next  = day_glob(COM_BASE, next_day)

    # Load submissions for this day only (bounded)
    submissions_df = con.execute(
        f"SELECT * FROM read_parquet('{submissions_glob}')"
    ).df()

    if submissions_df.empty:
        print(f"[{sub_day}] no submissions; skipping.")
        continue

    submissions = submissions_df.to_dict("records")
    sub_idx = build_sub_index(submissions)

    # Only allow comment groups for submissions from this day
    allowed = set(sub_idx.keys())

    # Stream comments from sub_day and next_day together
    # DuckDB can read multiple globs via UNION ALL.
    # We pass one combined relation to your iterator by using a query view.
    con.execute("DROP VIEW IF EXISTS comments_two_days")
    con.execute(f"""
        CREATE VIEW comments_two_days AS
        SELECT * FROM read_parquet('{comments_glob_today}')
        UNION ALL
        SELECT * FROM read_parquet('{comments_glob_next}')
    """)

    # Now stream from that view. We need a glob/path argument; easiest is to make
    # a tiny wrapper iterator that calls the same SQL but over the view.
    # If you don’t want to change iter_comments_grouped_duckdb, see the note below.
    def iter_groups_two_days(batch_rows: int = 50_000):
        # Same idea as iter_comments_grouped_duckdb but reading from the view
        sub_expr = """
        CASE
          WHEN position('_' IN link_id) > 0 AND length(split_part(link_id, '_', 1)) = 2
            THEN split_part(link_id, '_', 2)
          ELSE link_id
        END
        """
        q = f"""
          SELECT
            {sub_expr} AS submission_id,
            *
          FROM comments_two_days
          WHERE link_id IS NOT NULL
          ORDER BY submission_id
        """
        cur = con.cursor()
        cur.execute(q)
        reader = cur.fetch_record_batch(int(batch_rows))

        import pyarrow as pa

        cur_sid = None
        buf = []
        for rb in reader:
            rows = pa.Table.from_batches([rb]).to_pylist()
            for r in rows:
                sid = r.get("submission_id")
                if not sid:
                    continue
                if cur_sid is None:
                    cur_sid = sid
                if sid != cur_sid:
                    yield cur_sid, buf
                    buf = []
                    cur_sid = sid
                buf.append(r)
        if cur_sid is not None and buf:
            yield cur_sid, buf

    comment_groups = iter_groups_two_days(batch_rows=50_000)

    # Filter to just submissions from sub_day
    comment_groups = filter_groups_to_submission_set(comment_groups, allowed)

    # Write day-partitioned outputs
    day_out_dir = OUT_DIR / f"year={sub_day.year}" / f"month={sub_day.month}" / f"day={sub_day.day}"

    paths = build_observations_batched_stream(
        comment_groups,
        submissions=submissions,
        rows_per_file=50_000,
        file_format="parquet",
        file_prefix="observations",
        out_dir=day_out_dir,
    )

    print(f"[{sub_day}] wrote {len(paths)} files to {day_out_dir}")