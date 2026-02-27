import os
import re
import math
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union, Callable, Iterator
from pathlib import Path
import duckdb
import pyarrow as pa

import pandas as pd
from transformers import pipeline
import torch
from dataclasses import dataclass

from datasets import Dataset
from transformers import (
    AutoModelForSequenceClassification,
    AutoTokenizer,
    DataCollatorWithPadding,
)
from torch.utils.data import DataLoader

try:
    from zoneinfo import ZoneInfo  # py3.9+
except ImportError:
    ZoneInfo = None


# ----------------------------
# Configuration
# ----------------------------
load_dotenv()  # load .env file if present
FINBERT_MODEL_DIR = os.getenv("FINBERT_MODEL_DIR", "/opt/models/finbert")
TWITTER_ROBERTA_MODEL_DIR = os.getenv(
    "TWITTER_ROBERTA_MODEL_DIR", "/opt/models/twitter_roberta_sentiment"
)
STOCKS_PARQUET = os.getenv("STOCKS_PARQUET", "/opt/data/merged.parquet")

BATCH_SIZE_TWITTER = int(os.getenv("BATCH_SIZE_TWITTER", "64"))
BATCH_SIZE_FINBERT = int(os.getenv("BATCH_SIZE_FINBERT", "32"))

# Chunk size for *your* outer loop (pipeline still batches internally)
CHUNK_SIZE_ITEMS = int(os.getenv("CHUNK_SIZE_ITEMS", "2048"))

DECAY_BASE = float(os.getenv("SENTIMENT_DECAY_BASE", "0.7"))
MAX_DEPTH = int(os.getenv("MAX_COMMENT_DEPTH", "20"))

EASTERN_TZ = os.getenv("MARKET_TZ", "America/New_York")

TITLE_WEIGHT_MULT = float(os.getenv("TITLE_WEIGHT_MULT", "1.5"))
SELFTEXT_WEIGHT_MULT = float(os.getenv("SELFTEXT_WEIGHT_MULT", "1.0"))

# ignore comments created more than this many hours after submission time
COMMENT_CUTOFF_HOURS = float(os.getenv("COMMENT_CUTOFF_HOURS", "24"))

# Output batching (new)
OUTPUT_DIR_DEFAULT = os.getenv("OBS_OUTPUT_DIR")
OUTPUT_FORMAT_DEFAULT = os.getenv("OBS_OUTPUT_FORMAT", "parquet").lower()  # parquet|csv
OUTPUT_ROWS_PER_FILE_DEFAULT = int(os.getenv("OBS_OUTPUT_ROWS_PER_FILE", "10000"))
OUTPUT_FILE_PREFIX_DEFAULT = os.getenv("OBS_OUTPUT_PREFIX", "observations")

SORT_BEFORE_WRITE_DEFAULT = bool(os.getenv("SORT_BEFORE_WRITE", "False"))

MIN_SUBMISSION_COMMENTS = int(os.getenv("MIN_SUBMISSION_COMMENTS", "5"))
MIN_SUBMISSION_SCORE = int(os.getenv("MIN_SUBMISSION_SCORE", "10"))

if torch.cuda.is_available():
    DEVICE = 0
    torch.backends.cuda.matmul.allow_tf32 = True
    torch.set_float32_matmul_precision("high")
else:
    DEVICE = -1

# If a token is ambiguous (common English word / preposition / etc), we require
# that it appears as ALL CAPS (e.g., "ON" not "on") OR as a cashtag ($ON).
AMBIGUOUS_TICKERS: Set[str] = {
    "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", 
    "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z",
    "AN", "AS", "AT", "BE", "BY", "DO", "FOR", "FROM", "GO", "IF", "IN", "IS", "IT",
    "NO", "NOT", "OF", "ON", "OR", "SO", "THE", "TO", "UP", "US", "WE", "YOU", "HE", "SHE",
    "THEY", "HARD", "HIGH", "GOOD", "WAY", "ARE", "CAN", "ALL", "AM", "ANY", "AWAY", "BEAT",
    "USE", "POST", "LOW", "NEW", "OLD", "NOW", "PAY", "RUN", "SEE", "SET", "TOP", "WIN", 
    "YES", "BUY", "SELL", "CALL", "PUT", "FUND", "FUNDS", "BOND", "BONDS", "CASH", "COST", 
    "RATE", "JUST", "LOT", "WEEK", "PLAY", "HERE", "BULL", "BEAR", "OUT", "LIFE", "HELP", 
    "HOPE", "LIKE", "LOVE", "GLAD", "WWW", "NEXT", "SURE", "NICE", "JUST", "GAIN", "COM", 
    "VEGA", "NEAR", "TINY", "TALK", "KNOW", "MADE", "ELSE", "GROW", "BIT", "TRIP", "TIME", 
    "SAY", "HAS", "DIVE", "MIND", "WANT", "FUN", "SPAM", "SITE", "EM", "OPEN", "EASY", "EDIT", 
    "ITM", "OTM", "TWO", "MOVE", "MAN", "GL", "REAL", "MOVE", "NOTE", "LEAD", "YEAR", "HOUR", 
    "ODDS", "RARE", "AGO", "EVER", "WELL", "MINE", "EDGE", "FAN", "MAX", "MIN"
}

COMPANY_ALIASES: Dict[str, str] = {
    "google": "GOOG",
}


# ----------------------------
# Lazy globals
# ----------------------------
_finbert_tok = None
_finbert_model = None

_twitter_tok = None
_twitter_model = None

_stocks_df = None
_symbol_regex = None
_company_regex = None
_cashtag_regex = None

_symbol_col_name = None
_company_col_name = None
_name_to_symbol: Dict[str, str] = {}  # normalized company name -> ticker


# ----------------------------
# Helpers
# ----------------------------
def _chunks(iterable: Iterable, n: int):
    it = iter(iterable)
    while True:
        chunk = []
        try:
            for _ in range(n):
                chunk.append(next(it))
        except StopIteration:
            if chunk:
                yield chunk
            break
        yield chunk


def _normalize_fullname(x: Any) -> str:
    if x is None:
        return ""
    return str(x).strip()


def _base_id(x: Any) -> str:
    """
    Convert:
      - 't1_abc' -> 'abc'
      - 't3_def' -> 'def'
      - 'abc' -> 'abc'
    """
    if x is None:
        return ""
    s = str(x).strip()
    if "_" in s and len(s.split("_", 1)[0]) == 2:
        return s.split("_", 1)[1]
    return s


def _is_submission_fullname(x: str) -> bool:
    return str(x).startswith("t3_")


def _to_eastern_dt(created: Any) -> datetime:
    """
    Convert epoch seconds / datetime / pandas timestamp / string into aware datetime in America/New_York.
    Handles your parquet created_ts that already includes an offset.
    """
    if ZoneInfo is None:
        raise RuntimeError(
            "zoneinfo not available; use Python 3.9+ runtime for America/New_York conversion"
        )

    tz_et = ZoneInfo(EASTERN_TZ)

    if isinstance(created, (int, float)) and not (
        isinstance(created, float) and math.isnan(created)
    ):
        dt_utc = datetime.fromtimestamp(float(created), tz=timezone.utc)
        return dt_utc.astimezone(tz_et)

    if isinstance(created, datetime):
        dt = created
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(tz_et)

    ts = pd.to_datetime(created, utc=False, errors="coerce")
    if pd.isna(ts):
        return datetime.now(timezone.utc).astimezone(tz_et)

    dt = ts.to_pydatetime()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(tz_et)


def decay_weight(depth: int) -> float:
    return float(DECAY_BASE) ** int(depth)


def _is_ambiguous_ticker(tkr: str) -> bool:
    return str(tkr).upper() in AMBIGUOUS_TICKERS


def _clean_text(x: Any) -> str:
    if not x:
        return ""
    if isinstance(x, bytes):
        x = x.decode("utf-8", errors="ignore")
    return " ".join(str(x).split())


def _ensure_dir(p: Union[str, Path]) -> Path:
    out = Path(p).expanduser().resolve()
    out.mkdir(parents=True, exist_ok=True)
    return out


def _write_batch_rows(
    rows: List[Dict[str, Any]],
    out_dir: Union[str, Path],
    batch_idx: int,
    fmt: str = "parquet",
    prefix: str = OUTPUT_FILE_PREFIX_DEFAULT,
) -> Path:
    """
    Write a batch of row dicts to disk as a small dataframe, then return the path.
    Prefers parquet; falls back to csv if parquet writer isn't available.
    """
    out_dir_p = _ensure_dir(out_dir)
    fmt = (fmt or "parquet").lower().strip()

    df = pd.DataFrame.from_records(rows)

    if fmt == "parquet":
        path = out_dir_p / f"{prefix}_{batch_idx:07d}.parquet"
        try:
            df.to_parquet(path, index=False)
            return path
        except Exception as e:
            # Parquet engine not installed (pyarrow/fastparquet), or other IO error.
            print(
                f"Warning: failed to write parquet ({e}); falling back to CSV for this batch."
            )
            path = out_dir_p / f"{prefix}_{batch_idx:06d}.csv"
            df.to_csv(path, index=False)
            return path

    # csv
    path = out_dir_p / f"{prefix}_{batch_idx:06d}.csv"
    df.to_csv(path, index=False)
    return path

def _build_observations_batched_from_groups(
    comment_groups: Iterable[Tuple[str, List[Dict[str, Any]]]],
    submissions: Optional[List[Dict[str, Any]]] = None,
    *,
    out_dir: Union[str, Path] = OUTPUT_DIR_DEFAULT,
    rows_per_file: int = OUTPUT_ROWS_PER_FILE_DEFAULT,
    file_format: str = OUTPUT_FORMAT_DEFAULT,
    file_prefix: str = OUTPUT_FILE_PREFIX_DEFAULT,
    sort_before_write: bool = SORT_BEFORE_WRITE_DEFAULT,
) -> List[Path]:
    """
    Core implementation that works with a stream of (submission_id, sub_comments) groups.
    This is the "streaming entry point" core.
    """

    submissions = submissions or []

    _load_stocks()
    fin_tok, fin_model = _get_finbert_model()
    tw_tok, tw_model = _get_twitter_roberta_model()

    fin_runner = _ModelRunner(tokenizer=fin_tok, model=fin_model, batch_size=BATCH_SIZE_FINBERT)
    tw_runner  = _ModelRunner(tokenizer=tw_tok, model=tw_model, batch_size=BATCH_SIZE_TWITTER)

    sub_idx = _index_submissions(submissions)

    paths: List[Path] = []
    batch_rows: List[Dict[str, Any]] = []
    batch_idx = 0
    total_late_dropped = 0

    for submission_id, sub_comments in comment_groups:
        sub = sub_idx.get(submission_id) or sub_idx.get(f"t3_{submission_id}")

        # Filter by overall submission activity (from metadata)
        if not _submission_passes_min_activity(sub):
            continue

        # Compute depths only for this submission's comments
        depths_by_base = compute_depths(sub_comments)

        agg_sub: Dict[Tuple[str, str], Dict[str, Any]] = {}
        chunk_items: List[Dict[str, Any]] = []
        late_dropped = 0

        try:
            for c in sub_comments:
                if _is_comment_too_late(c, sub):
                    late_dropped += 1
                    continue

                body = _clean_text(c.get("body") or "")
                if not body:
                    continue

                cid_base = _base_id(c.get("id"))
                depth = depths_by_base.get(cid_base, 0)
                w = decay_weight(depth)

                chunk_items.append(
                    {
                        "submission_id": submission_id,
                        "ticker_text": body,
                        "author": c.get("author") if c.get("author") is not None else "[deleted]",
                        "comment_score": float(c.get("score")) if c.get("score") is not None else 0.0,
                        "depth": depth,
                        "weight": w,
                        "kind": "comment",
                    }
                )

                if len(chunk_items) >= CHUNK_SIZE_ITEMS:
                    _process_items_chunk_datasets(fin_runner, tw_runner, agg_sub, chunk_items)
                    chunk_items = []

            if chunk_items:
                _process_items_chunk_datasets(fin_runner, tw_runner, agg_sub, chunk_items)

        except torch.OutOfMemoryError:
            # Optional robustness: flush partial results for this submission then continue
            if torch.cuda.is_available():
                torch.cuda.empty_cache()
                torch.cuda.ipc_collect()

            if agg_sub:
                sub_feats_cache = {submission_id: _submission_features(sub)}
                rows_iterable = _iter_agg_rows(agg_sub, sub_feats_cache)
                batch_idx = _append_rows_to_batched_files(
                    rows_iterable=rows_iterable,
                    out_dir=out_dir,
                    rows_per_file=rows_per_file,
                    fmt=file_format,
                    prefix=file_prefix,
                    sort_before_write=sort_before_write,
                    batch_rows=batch_rows,
                    batch_idx=batch_idx,
                    paths=paths,
                )
            print(f"[OOM] submission={submission_id}: flushed partial results and continued.")
            continue

        total_late_dropped += late_dropped

        # Pseudo-items (title/selftext)
        pseudo_items = _make_submission_pseudo_rows_one(sub)
        for pseudo_chunk in _chunks(pseudo_items, CHUNK_SIZE_ITEMS):
            _process_items_chunk_datasets(fin_runner, tw_runner, agg_sub, pseudo_chunk)

        if not agg_sub:
            continue

        sub_feats_cache = {submission_id: _submission_features(sub)}
        rows_iterable = _iter_agg_rows(agg_sub, sub_feats_cache)
        batch_idx = _append_rows_to_batched_files(
            rows_iterable=rows_iterable,
            out_dir=out_dir,
            rows_per_file=rows_per_file,
            fmt=file_format,
            prefix=file_prefix,
            sort_before_write=sort_before_write,
            batch_rows=batch_rows,
            batch_idx=batch_idx,
            paths=paths,
        )

    # Flush tail
    if batch_rows:
        path = _write_batch_rows(
            batch_rows,
            out_dir=out_dir,
            batch_idx=batch_idx,
            fmt=file_format,
            prefix=file_prefix,
        )
        paths.append(path)
        batch_rows.clear()

    if total_late_dropped:
        print(f"Dropped {total_late_dropped} late comments (> {COMMENT_CUTOFF_HOURS}h after submission).")

    return paths


# ----------------------------
# Model pipelines
# ----------------------------
def _is_local_path(p: str) -> bool:
    try:
        return Path(p).exists()
    except Exception:
        return False

def _load_textcls_model(model_ref: str):
    local_only = _is_local_path(model_ref)
    tok = AutoTokenizer.from_pretrained(model_ref, local_files_only=local_only, use_fast=True)
    model = AutoModelForSequenceClassification.from_pretrained(model_ref, local_files_only=local_only)
    model.eval()

    if torch.cuda.is_available():
        model.to("cuda")
    return tok, model


def _get_finbert_model():
    global _finbert_tok, _finbert_model
    if _finbert_model is None or _finbert_tok is None:
        _finbert_tok, _finbert_model = _load_textcls_model(FINBERT_MODEL_DIR)
    return _finbert_tok, _finbert_model


def _get_twitter_roberta_model():
    global _twitter_tok, _twitter_model
    if _twitter_model is None or _twitter_tok is None:
        _twitter_tok, _twitter_model = _load_textcls_model(TWITTER_ROBERTA_MODEL_DIR)
    return _twitter_tok, _twitter_model

def _iter_comments_by_submission(
    comments: List[Dict[str, Any]],
    *,
    assume_grouped: bool = False,
    sort_if_needed: bool = True,
):
    """
    Yield (submission_id, [comments_for_submission]) pairs.

    - If assume_grouped=True, we treat the input as already grouped by link_id
      and stream with O(max_comments_in_one_submission) memory.
    - If assume_grouped=False and sort_if_needed=True, we sort by submission_id
      first (requires holding list refs, O(N log N)).
    """
    if not comments:
        return
        yield  # make generator

    def _sub_id(c: Dict[str, Any]) -> str:
        return _base_id(c.get("link_id"))

    if not assume_grouped:
        if not sort_if_needed:
            raise ValueError(
                "comments are not assumed grouped; set sort_if_needed=True "
                "or provide comments grouped by link_id."
            )
        # Sorting creates a new list of references; you still avoid the huge global agg
        comments = sorted(comments, key=_sub_id)

    cur_sid = None
    buf: List[Dict[str, Any]] = []

    for c in comments:
        sid = _sub_id(c)
        if not sid:
            continue

        if cur_sid is None:
            cur_sid = sid

        if sid != cur_sid:
            yield cur_sid, buf
            buf = []
            cur_sid = sid

        buf.append(c)

    if cur_sid is not None and buf:
        yield cur_sid, buf

def _base_id_sql(col: str) -> str:
    # DuckDB: split_part(str, '_', 2) gets "abc" from "t3_abc"
    # If there's no underscore, we just use the original string
    return f"""
    CASE
      WHEN position('_' IN {col}) > 0 AND length(split_part({col}, '_', 1)) = 2
        THEN split_part({col}, '_', 2)
      ELSE {col}
    END
    """

# ----------------------------
# DuckDB streaming (Arrow) + column-pruned conversion (FASTER)
# ----------------------------

# Minimal set of comment columns your pipeline uses.
# (submission_id is computed in SQL)
_COMMENT_COLS_NEEDED = [
    "id",
    "parent_id",
    "link_id",
    "author",
    "body",
    "score",
    "created_utc",
    "created_ts",
]

def _parquet_columns_duckdb(con: duckdb.DuckDBPyConnection, parquet_glob: str) -> Set[str]:
    """
    Get column names for the parquet glob with minimal IO.
    DuckDB reads metadata/footers for LIMIT 0, not the full dataset.
    """
    t = con.execute(f"SELECT * FROM read_parquet('{parquet_glob}') LIMIT 0").fetch_arrow_table()
    return set(t.schema.names)

def _rows_from_recordbatch_pruned(rb: pa.RecordBatch, cols: List[str]) -> Iterator[Dict[str, Any]]:
    """
    Convert only selected columns in this RecordBatch into row dicts.
    Fast path: convert each needed column to a python list once, then zip by index.
    """
    # Note: rb.column(i).to_pylist() is typically faster than per-row scalar.as_py()
    data = {c: rb.column(rb.schema.get_field_index(c)).to_pylist() for c in cols}
    n = rb.num_rows
    for i in range(n):
        yield {c: data[c][i] for c in cols}

def iter_comments_grouped_duckdb(
    con: duckdb.DuckDBPyConnection,
    comments_glob: str,
    *,
    batch_rows: int = 50_000,
) -> Iterator[Tuple[str, List[Dict[str, Any]]]]:
    """
    True streaming (no OFFSET) with lower overhead:
      - cursor -> fetch_record_batch
      - SQL selects ONLY required columns (+ computed submission_id)
      - per Arrow RecordBatch: convert only needed columns to Python lists once
      - yields grouped (submission_id, [comment_dicts]) without loading all comments

    Assumes:
      - link_id exists
      - submission_id = base_id(link_id) (computed in SQL)
      - ORDER BY submission_id ensures grouping correctness across batches
    """
    # Figure out which optional timestamp columns exist (created_utc / created_ts)
    available = _parquet_columns_duckdb(con, comments_glob)

    # Hard requirements for your downstream logic
    required = {"id", "parent_id", "link_id", "author", "body", "score"}
    missing_req = [c for c in required if c not in available]
    if missing_req:
        raise ValueError(
            f"Comments parquet is missing required columns: {missing_req}. "
            f"Available columns include: {sorted(list(available))[:50]} ..."
        )

    # Only keep columns that actually exist (robust to created_ts/created_utc differences)
    cols = [c for c in _COMMENT_COLS_NEEDED if c in available]

    sub_expr = _base_id_sql("link_id")

    # Column-pruned query (this is the main win vs SELECT *)
    select_cols_sql = ",\n        ".join(cols)

    q = f"""
      SELECT
        {sub_expr} AS submission_id,
        {select_cols_sql}
      FROM read_parquet('{comments_glob}')
      WHERE link_id IS NOT NULL
      ORDER BY submission_id
    """

    cur = con.cursor()
    cur.execute(q)

    reader = cur.fetch_record_batch(int(batch_rows))

    cur_sid: Optional[str] = None
    buf: List[Dict[str, Any]] = []

    # We will convert only these columns from each RecordBatch
    pruned_cols = ["submission_id"] + cols

    for rb in reader:  # rb is a pyarrow.RecordBatch
        # rb already contains only the selected columns, but we still avoid .to_pylist() on full rows.
        for r in _rows_from_recordbatch_pruned(rb, pruned_cols):
            sid = r.get("submission_id")
            if not sid:
                continue

            if cur_sid is None:
                cur_sid = sid

            if sid != cur_sid:
                yield cur_sid, buf
                buf = []
                cur_sid = sid

            # Drop submission_id from each comment dict (your downstream uses submission_id from the group key)
            # If you prefer keeping it in each dict, remove the pop().
            r.pop("submission_id", None)

            buf.append(r)

    if cur_sid is not None and buf:
        yield cur_sid, buf

def load_submissions_index_duckdb(con, submissions_glob: str) -> Dict[str, Dict[str, Any]]:
    q = f"SELECT * FROM read_parquet('{submissions_glob}')"
    df = con.execute(q).df()
    subs = df.to_dict("records")

    idx = {}
    for s in subs:
        sid = str(s.get("id", "")).strip()
        if not sid:
            continue
        # keep both t3_abc and abc forms
        if sid.startswith("t3_"):
            idx[sid[3:]] = s
            idx[sid] = s
        else:
            idx[sid] = s
            idx[f"t3_{sid}"] = s
    return idx

# ----------------------------
# Stocks parquet -> regexes + company->ticker mapping
# ----------------------------
def _normalize_company_key(name: str) -> str:
    return " ".join(str(name).strip().lower().split())


def _load_stocks():
    global _stocks_df, _symbol_regex, _company_regex, _cashtag_regex
    global _symbol_col_name, _company_col_name, _name_to_symbol

    if _stocks_df is not None:
        return _stocks_df

    try:
        df = pd.read_parquet(STOCKS_PARQUET)
    except Exception as e:
        df = pd.DataFrame(columns=["symbol", "name"])
        print(f"Warning: could not read stocks parquet at {STOCKS_PARQUET}: {e}")

    cols = {c.lower(): c for c in df.columns}
    _symbol_col_name = cols.get("symbol") or cols.get("ticker")
    _company_col_name = cols.get("name") or cols.get("company_name")

    symbols: List[str] = []
    company_names: List[str] = []

    if _symbol_col_name and _symbol_col_name in df.columns:
        symbols = (
            df[_symbol_col_name].dropna().astype(str).str.upper().unique().tolist()
        )

    _name_to_symbol = {}

    if (
        _company_col_name
        and _symbol_col_name
        and _company_col_name in df.columns
        and _symbol_col_name in df.columns
    ):
        tmp = df[[_company_col_name, _symbol_col_name]].dropna()

        for comp, sym in tmp.itertuples(index=False):
            ck = _normalize_company_key(comp)
            if ck:
                _name_to_symbol.setdefault(ck, str(sym).upper())

        company_names = tmp[_company_col_name].astype(str).str.strip().unique().tolist()
        company_names = [n for n in company_names if 2 <= len(n) <= 100]

    for alias_name, alias_sym in COMPANY_ALIASES.items():
        _name_to_symbol.setdefault(_normalize_company_key(alias_name), alias_sym.upper())

    alias_company_names = [name for name in COMPANY_ALIASES.keys() if 2 <= len(name) <= 100]
    company_names = list(set(company_names + alias_company_names))

    _stocks_df = df

    _cashtag_regex = re.compile(r"(?P<cashtag>\$[A-Z]{1,6}\b)")

    if symbols:
        symbols_sorted = sorted(set(symbols), key=lambda s: -len(s))
        symbols_escaped = [re.escape(s) for s in symbols_sorted]
        symbol_pattern = r"\b(?P<symbol>(" + "|".join(symbols_escaped) + r"))\b"
        _symbol_regex = re.compile(symbol_pattern, flags=re.IGNORECASE)
    else:
        _symbol_regex = re.compile(r"$^")

    if company_names:
        names_sorted = sorted(set(company_names), key=lambda n: -len(n))[:300]
        names_escaped = [re.escape(n) for n in names_sorted if n.strip()]
        company_pattern = r"\b(?P<company>(" + "|".join(names_escaped) + r"))\b"
        try:
            _company_regex = re.compile(company_pattern, flags=re.IGNORECASE)
        except re.error:
            _company_regex = None
    else:
        _company_regex = None

    return _stocks_df

def _batched(iterable, n):
    for i in range(0, len(iterable), n):
        yield iterable[i:i+n]

@dataclass
class _ModelRunner:
    tokenizer: Any
    model: Any
    batch_size: int
    max_length: int = 256
    amp_dtype: Any = torch.float16

    def predict_proba_3way(self, texts: List[str]) -> List[Dict[str, float]]:
        if not texts:
            return []

        device = torch.device("cuda") if torch.cuda.is_available() else torch.device("cpu")

        # label mapping
        id2label = getattr(self.model.config, "id2label", None) or {}
        def _norm_label(s: str) -> str:
            s = str(s).strip().lower()
            if s in ("label_0", "neg", "negative"):
                return "negative"
            if s in ("label_1", "neu", "neutral"):
                return "neutral"
            if s in ("label_2", "pos", "positive"):
                return "positive"
            return s

        idx_to_key = {}
        for i in range(int(getattr(self.model.config, "num_labels", 3))):
            idx_to_key[i] = _norm_label(id2label.get(i, f"label_{i}"))
        if not (set(idx_to_key.values()) >= {"negative", "neutral", "positive"}):
            idx_to_key = {0: "negative", 1: "neutral", 2: "positive"}

        bs = int(self.batch_size)
        while True:
            try:
                out: List[Dict[str, float]] = []
                use_amp = torch.cuda.is_available()

                with torch.inference_mode():
                    for chunk in _batched(texts, bs):
                        enc = self.tokenizer(
                            chunk,
                            padding=True,
                            truncation=True,
                            max_length=int(self.max_length),
                            return_tensors="pt",
                        )
                        enc = {k: v.to(device, non_blocking=True) for k, v in enc.items()}

                        if use_amp:
                            with torch.autocast(device_type="cuda", dtype=self.amp_dtype):
                                logits = self.model(**enc).logits
                        else:
                            logits = self.model(**enc).logits

                        probs = torch.softmax(logits, dim=-1).detach().cpu()

                        for row in probs:
                            d = {"negative": 0.0, "neutral": 0.0, "positive": 0.0}
                            for i, p in enumerate(row.tolist()):
                                key = idx_to_key.get(i)
                                if key in d:
                                    d[key] = float(p)
                            out.append(d)

                return out

            except torch.OutOfMemoryError:
                if not torch.cuda.is_available():
                    raise
                torch.cuda.empty_cache()
                torch.cuda.ipc_collect()
                bs = max(1, bs // 2)
                if bs == 1:
                    raise


def extract_tickers_from_text(text: str) -> List[str]:
    """
    Matching rules:
      - cashtags ($GOOG) always count
      - symbol matches are case-insensitive, BUT if the symbol is ambiguous (common word),
        it must appear as ALL CAPS in the original text to count (e.g., "ON" not "on")
      - company name matches are case-insensitive and map via _name_to_symbol
        (includes forced aliases Google/Alphabet -> GOOG)
    """
    if not text:
        return []
    _load_stocks()

    tickers: Set[str] = set()

    for m in _cashtag_regex.finditer(text):
        tickers.add(m.group("cashtag").lstrip("$").upper())

    for m in _symbol_regex.finditer(text):
        surface = m.group("symbol")
        sym = surface.upper()

        if _is_ambiguous_ticker(sym):
            if not surface.isupper():
                continue

        tickers.add(sym)

    if _company_regex and _name_to_symbol:
        for m in _company_regex.finditer(text):
            comp_surface = m.group("company").strip()
            sym = _name_to_symbol.get(_normalize_company_key(comp_surface))
            if sym:
                tickers.add(sym.upper())

    return sorted(tickers)


# ----------------------------
# Depth computation (memory-lean: store only parent/link)
# ----------------------------
def compute_depths(comments: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Compute depth per comment base-id, capped by MAX_DEPTH.
    by_base[cid] = (parent_id_fullname, link_id_fullname)
    """
    by_base: Dict[str, Tuple[str, str]] = {}
    for c in comments:
        cid_base = _base_id(c.get("id"))
        if not cid_base:
            continue
        parent_id = _normalize_fullname(c.get("parent_id"))
        link_id = _normalize_fullname(c.get("link_id"))
        by_base[cid_base] = (parent_id, link_id)

    depths: Dict[str, int] = {}

    def _depth_for_base(cid_base: str) -> int:
        if cid_base in depths:
            return depths[cid_base]

        pl = by_base.get(cid_base)
        if pl is None:
            depths[cid_base] = 0
            return 0

        parent_id, link_id = pl

        if parent_id and link_id and parent_id == link_id:
            d = 0
        elif _is_submission_fullname(parent_id):
            d = 0
        else:
            parent_base = _base_id(parent_id)
            if not parent_base or parent_base not in by_base:
                d = 0
            else:
                d = min(MAX_DEPTH, _depth_for_base(parent_base) + 1)

        depths[cid_base] = d
        return d

    for c in comments:
        cid_base = _base_id(c.get("id"))
        if cid_base:
            _depth_for_base(cid_base)

    return depths


# ----------------------------
# Submissions indexing + features + timestamps
# ----------------------------
def _index_submissions(submissions: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    idx: Dict[str, Dict[str, Any]] = {}
    for s in submissions or []:
        sid_base = _base_id(s.get("id"))
        if not sid_base:
            continue
        idx[sid_base] = s
        idx[f"t3_{sid_base}"] = s
    return idx


def _get_submission_created_et(sub: Optional[Dict[str, Any]]) -> Optional[datetime]:
    if not sub:
        return None
    created_src = sub.get("created_utc")
    if created_src is None:
        created_src = sub.get("created_ts")
    if created_src is None:
        return None
    try:
        return _to_eastern_dt(created_src)
    except Exception:
        return None


def _submission_features(sub: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    created_et = _get_submission_created_et(sub)
    return {
        "submission_score": float(sub.get("score")) if sub and sub.get("score") is not None else None,
        "submission_num_comments": int(sub.get("num_comments")) if sub and sub.get("num_comments") is not None else None,
        "submission_upvote_ratio": float(sub.get("upvote_ratio")) if sub and sub.get("upvote_ratio") is not None else None,
        "submission_created_et": created_et.isoformat() if created_et else None,
    }


# ----------------------------
# Pseudo comment rows for title/selftext
# ----------------------------
def _make_submission_pseudo_rows_one(sub: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not sub:
        return []
    sid_base = _base_id(sub.get("id"))
    if not sid_base:
        return []

    author = sub.get("author") if sub.get("author") is not None else "[deleted]"
    title = _clean_text(sub.get("title") or "")
    selftext = _clean_text(sub.get("selftext") or "")

    pseudo: List[Dict[str, Any]] = []
    if title:
        pseudo.append(
            {
                "submission_id": sid_base,
                "ticker_text": title,
                "author": author,
                "comment_score": 0.0,
                "depth": 0,
                "weight": 1.0 * TITLE_WEIGHT_MULT,
                "kind": "submission_title",
            }
        )

    if selftext:
        pseudo.append(
            {
                "submission_id": sid_base,
                "ticker_text": selftext,
                "author": author,
                "comment_score": 0.0,
                "depth": 0,
                "weight": 1.0 * SELFTEXT_WEIGHT_MULT,
                "kind": "submission_selftext",
            }
        )

    return pseudo


# ----------------------------
# Comment cutoff: ignore comments > 24h after submission
# ----------------------------
def _is_comment_too_late(
    comment: Dict[str, Any],
    submission: Optional[Dict[str, Any]],
    cutoff_hours: float = COMMENT_CUTOFF_HOURS,
) -> bool:
    """
    Returns True if comment was created more than cutoff_hours after submission creation time.
    Fail-open: if timestamps/submission missing, returns False (do not drop).
    """
    sub_created = _get_submission_created_et(submission)
    if sub_created is None:
        return False

    c_created_src = comment.get("created_utc")
    if c_created_src is None:
        c_created_src = comment.get("created_ts")
    if c_created_src is None:
        return False

    try:
        c_created_et = _to_eastern_dt(c_created_src)
    except Exception:
        return False

    return c_created_et > (sub_created + timedelta(hours=float(cutoff_hours)))


# ----------------------------
# Aggregation (streaming-friendly)
# ----------------------------
_PROB_COLS = ["fin_neg", "fin_neu", "fin_pos", "tw_neg", "tw_neu", "tw_pos"]


def _agg_init(submission_id: str, ticker: str) -> Dict[str, Any]:
    return {
        "submission_id": submission_id,
        "ticker": ticker,
        "n_items": 0,
        "authors": set(),  # exact distinct authors (can be memory-heavy at huge scale)
        "comment_score_sum": 0.0,
        "comment_score_max": float("-inf"),
        "depth_sum": 0.0,
        "n_from_comments": 0,
        "n_from_title": 0,
        "n_from_selftext": 0,
        "weight_sum": 0.0,  # denom for weighted means
        "fin_neg_wsum": 0.0,
        "fin_neu_wsum": 0.0,
        "fin_pos_wsum": 0.0,
        "tw_neg_wsum": 0.0,
        "tw_neu_wsum": 0.0,
        "tw_pos_wsum": 0.0,
    }


def _agg_update(
    agg: Dict[Tuple[str, str], Dict[str, Any]],
    submission_id: str,
    ticker: str,
    author: str,
    comment_score: float,
    depth: int,
    weight: float,
    kind: str,
    fin_scores: Dict[str, float],
    tw_scores: Dict[str, float],
):
    key = (submission_id, ticker)
    a = agg.get(key)
    if a is None:
        a = _agg_init(submission_id, ticker)
        agg[key] = a

    a["n_items"] += 1
    a["authors"].add(author)

    a["comment_score_sum"] += float(comment_score)
    a["comment_score_max"] = max(a["comment_score_max"], float(comment_score))
    a["depth_sum"] += float(depth)

    if kind == "comment":
        a["n_from_comments"] += 1
    elif kind == "submission_title":
        a["n_from_title"] += 1
    elif kind == "submission_selftext":
        a["n_from_selftext"] += 1

    w = float(weight)
    a["weight_sum"] += w

    a["fin_neg_wsum"] += fin_scores["negative"] * w
    a["fin_neu_wsum"] += fin_scores["neutral"] * w
    a["fin_pos_wsum"] += fin_scores["positive"] * w
    a["tw_neg_wsum"] += tw_scores["negative"] * w
    a["tw_neu_wsum"] += tw_scores["neutral"] * w
    a["tw_pos_wsum"] += tw_scores["positive"] * w


def _process_items_chunk_datasets(
    fin_runner: _ModelRunner,
    tw_runner: _ModelRunner,
    agg: Dict[Tuple[str, str], Dict[str, Any]],
    items: List[Dict[str, Any]],
    *,
    dedupe_texts_within_chunk: bool = True,
):
    if not items:
        return

    # 1) Extract tickers (cheap)
    filtered: List[Dict[str, Any]] = []
    for it in items:
        tickers = extract_tickers_from_text(it["ticker_text"])
        if not tickers:
            continue
        it2 = dict(it)
        it2["_tickers"] = tickers
        filtered.append(it2)

    if not filtered:
        return

    texts = [it["ticker_text"] for it in filtered]

    # 2) Optional dedupe to reduce model calls (often helps a lot with repeated content)
    if dedupe_texts_within_chunk:
        # map text -> first index and list of indices
        text_to_indices: Dict[str, List[int]] = {}
        unique_texts: List[str] = []
        for i, t in enumerate(texts):
            if t not in text_to_indices:
                text_to_indices[t] = []
                unique_texts.append(t)
            text_to_indices[t].append(i)

        fin_unique = fin_runner.predict_proba_3way(unique_texts)
        tw_unique = tw_runner.predict_proba_3way(unique_texts)

        fin_scores_all = [None] * len(texts)
        tw_scores_all = [None] * len(texts)

        for ut, fin_s, tw_s in zip(unique_texts, fin_unique, tw_unique):
            for i in text_to_indices[ut]:
                fin_scores_all[i] = fin_s
                tw_scores_all[i] = tw_s
    else:
        fin_scores_all = fin_runner.predict_proba_3way(texts)
        tw_scores_all = tw_runner.predict_proba_3way(texts)

    # 3) Aggregate
    for it, fin_s, tw_s in zip(filtered, fin_scores_all, tw_scores_all):
        fin_s = fin_s or {"negative": 0.0, "neutral": 0.0, "positive": 0.0}
        tw_s = tw_s or {"negative": 0.0, "neutral": 0.0, "positive": 0.0}

        for tkr in it["_tickers"]:
            _agg_update(
                agg=agg,
                submission_id=it["submission_id"],
                ticker=str(tkr).upper(),
                author=it["author"],
                comment_score=float(it["comment_score"]),
                depth=int(it["depth"]),
                weight=float(it["weight"]),
                kind=it["kind"],
                fin_scores=fin_s,
                tw_scores=tw_s,
            )


# ----------------------------
# Finalize aggregation -> batched files (NEW)
# ----------------------------
def _iter_agg_rows(
    agg: Dict[Tuple[str, str], Dict[str, Any]],
    sub_feats_cache: Dict[str, Dict[str, Any]],
):
    """
    Generator yielding finalized row dicts from agg.
    """
    for (submission_id, ticker), a in agg.items():
        n = int(a["n_items"])
        weight_sum = float(a["weight_sum"])
        denom = weight_sum if weight_sum != 0.0 else float("nan")

        row = {
            "submission_id": submission_id,
            "ticker": ticker,
            **sub_feats_cache.get(submission_id, {}),
            "n_items": n,
            "n_authors": int(len(a["authors"])),
            "comment_score_sum": float(a["comment_score_sum"]),
            "comment_score_mean": float(a["comment_score_sum"]) / n if n else float("nan"),
            "comment_score_max": float(a["comment_score_max"]) if n else float("nan"),
            "avg_depth": float(a["depth_sum"]) / n if n else float("nan"),
            "n_from_comments": int(a["n_from_comments"]),
            "n_from_title": int(a["n_from_title"]),
            "n_from_selftext": int(a["n_from_selftext"]),
            # weighted sums
            "fin_neg_sum_w": float(a["fin_neg_wsum"]),
            "fin_neu_sum_w": float(a["fin_neu_wsum"]),
            "fin_pos_sum_w": float(a["fin_pos_wsum"]),
            "tw_neg_sum_w": float(a["tw_neg_wsum"]),
            "tw_neu_sum_w": float(a["tw_neu_wsum"]),
            "tw_pos_sum_w": float(a["tw_pos_wsum"]),
            # weighted means
            "fin_neg_mean_w": float(a["fin_neg_wsum"]) / denom if weight_sum else float("nan"),
            "fin_neu_mean_w": float(a["fin_neu_wsum"]) / denom if weight_sum else float("nan"),
            "fin_pos_mean_w": float(a["fin_pos_wsum"]) / denom if weight_sum else float("nan"),
            "tw_neg_mean_w": float(a["tw_neg_wsum"]) / denom if weight_sum else float("nan"),
            "tw_neu_mean_w": float(a["tw_neu_wsum"]) / denom if weight_sum else float("nan"),
            "tw_pos_mean_w": float(a["tw_pos_wsum"]) / denom if weight_sum else float("nan"),
        }

        yield row


def _append_rows_to_batched_files(
    *,
    rows_iterable,
    out_dir: Union[str, Path],
    rows_per_file: int,
    fmt: str,
    prefix: str,
    sort_before_write: bool,
    batch_rows: List[Dict[str, Any]],
    batch_idx: int,
    paths: List[Path],
) -> int:
    """
    Append rows into the existing batch_rows buffer; flush to disk as needed.
    Returns updated batch_idx.
    """
    if sort_before_write:
        # rows_iterable is expected to be a list or generator; easiest is to materialize
        # per-submission (small) and sort in-memory.
        rows_list = list(rows_iterable)
        rows_list.sort(key=lambda r: (r.get("submission_id", ""), r.get("ticker", "")))
        rows_iterable = rows_list

    for row in rows_iterable:
        batch_rows.append(row)
        if len(batch_rows) >= int(rows_per_file):
            path = _write_batch_rows(
                batch_rows,
                out_dir=out_dir,
                batch_idx=batch_idx,
                fmt=fmt,
                prefix=prefix,
            )
            paths.append(path)
            batch_rows.clear()
            batch_idx += 1

    return batch_idx

def _submission_passes_min_activity(sub: Optional[Dict[str, Any]]) -> bool:
    if not sub:
        return False  # skip if submission metadata missing

    n_comments = sub.get("num_comments")
    score = sub.get("score")

    n_comments_i = int(n_comments) if n_comments is not None else 0
    score_i = int(score) if score is not None else 0

    return (
        n_comments_i >= MIN_SUBMISSION_COMMENTS
        and score_i >= MIN_SUBMISSION_SCORE
    )


# ----------------------------
# Main: now supports batched output to disk
# ----------------------------
def build_observations_batched(
    comments: List[Dict[str, Any]],
    submissions: Optional[List[Dict[str, Any]]] = None,
    *,
    out_dir: Union[str, Path] = OUTPUT_DIR_DEFAULT,
    rows_per_file: int = OUTPUT_ROWS_PER_FILE_DEFAULT,
    file_format: str = OUTPUT_FORMAT_DEFAULT,
    file_prefix: str = OUTPUT_FILE_PREFIX_DEFAULT,
    sort_before_write: bool = SORT_BEFORE_WRITE_DEFAULT,
    assume_comments_grouped_by_submission: bool = False,
    sort_comments_by_submission_if_needed: bool = True,
) -> List[Path]:
    """
    Original list-based entry point.
    """

    if not isinstance(comments, list):
        raise ValueError("comments must be a list of dicts")

    groups = _iter_comments_by_submission(
        comments,
        assume_grouped=assume_comments_grouped_by_submission,
        sort_if_needed=sort_comments_by_submission_if_needed,
    )

    return _build_observations_batched_from_groups(
        groups,
        submissions=submissions,
        out_dir=out_dir,
        rows_per_file=rows_per_file,
        file_format=file_format,
        file_prefix=file_prefix,
        sort_before_write=sort_before_write,
    )

def build_observations_batched_stream(
    comment_groups: Iterable[Tuple[str, List[Dict[str, Any]]]],
    submissions: Optional[List[Dict[str, Any]]] = None,
    *,
    out_dir: Union[str, Path] = OUTPUT_DIR_DEFAULT,
    rows_per_file: int = OUTPUT_ROWS_PER_FILE_DEFAULT,
    file_format: str = OUTPUT_FORMAT_DEFAULT,
    file_prefix: str = OUTPUT_FILE_PREFIX_DEFAULT,
    sort_before_write: bool = SORT_BEFORE_WRITE_DEFAULT,
) -> List[Path]:
    """
    Streaming entry point.

    comment_groups must yield:
      (submission_id, sub_comments_list)

    Where sub_comments_list contains ONLY comments for that submission.
    No sorting is performed here — the source should already be grouped.
    """
    return _build_observations_batched_from_groups(
        comment_groups,
        submissions=submissions,
        out_dir=out_dir,
        rows_per_file=rows_per_file,
        file_format=file_format,
        file_prefix=file_prefix,
        sort_before_write=sort_before_write,
    )


# ----------------------------
# Backwards-compatible wrapper (optional)
# ----------------------------
def build_observations_df(
    comments: List[Dict[str, Any]],
    submissions: Optional[List[Dict[str, Any]]] = None,
) -> pd.DataFrame:
    """
    Original behavior preserved: returns a single dataframe.
    If you want bounded memory, prefer build_observations_batched().
    """
    # Use the batched writer into a temp dir then read back and concat (not memory-friendly),
    # so we keep the original "single DF" code path by reusing your previous approach.
    # This is left intentionally simple to preserve prior expectations.
    if not isinstance(comments, list):
        raise ValueError("comments must be a list of dicts (depth computation needs full pass)")

    submissions = submissions or []

    _load_stocks()
    fin_tok, fin_model = _get_finbert_model()
    tw_tok, tw_model = _get_twitter_roberta_model()

    fin_runner = _ModelRunner(tokenizer=fin_tok, model=fin_model, batch_size=BATCH_SIZE_FINBERT)
    tw_runner = _ModelRunner(tokenizer=tw_tok, model=tw_model, batch_size=BATCH_SIZE_TWITTER)

    sub_idx = _index_submissions(submissions)
    depths_by_base = compute_depths(comments)

    agg: Dict[Tuple[str, str], Dict[str, Any]] = {}

    late_dropped = 0
    chunk_items: List[Dict[str, Any]] = []

    for c in comments:
        submission_id = _base_id(c.get("link_id"))
        sub = sub_idx.get(submission_id) or sub_idx.get(f"t3_{submission_id}")

        if _is_comment_too_late(c, sub):
            late_dropped += 1
            continue

        body = _clean_text(c.get("body") or "")
        if not body:
            continue

        cid_base = _base_id(c.get("id"))
        depth = depths_by_base.get(cid_base, 0)
        w = decay_weight(depth)

        chunk_items.append(
            {
                "submission_id": submission_id,
                "ticker_text": body,
                "author": c.get("author") if c.get("author") is not None else "[deleted]",
                "comment_score": float(c.get("score")) if c.get("score") is not None else 0.0,
                "depth": depth,
                "weight": w,
                "kind": "comment",
            }
        )

        if len(chunk_items) >= CHUNK_SIZE_ITEMS:
            _process_items_chunk_datasets(fin_runner, tw_runner, agg, chunk_items)
            chunk_items = []

    if chunk_items:
        _process_items_chunk_datasets(fin_runner, tw_runner, agg, chunk_items)

    if late_dropped:
        print(f"Dropped {late_dropped} late comments (> {COMMENT_CUTOFF_HOURS}h after submission).")

    sub = sub_idx.get(submission_id) or sub_idx.get(f"t3_{submission_id}")
    pseudo_items = _make_submission_pseudo_rows_one(sub)
    for pseudo_chunk in _chunks(pseudo_items, CHUNK_SIZE_ITEMS):
        _process_items_chunk_datasets(fin_runner, tw_runner, agg, pseudo_chunk)

    if not agg:
        return pd.DataFrame()

    sub_feats_cache: Dict[str, Dict[str, Any]] = {}
    sids = sorted({sid for (sid, _tkr) in agg.keys()})
    for sid in sids:
        sub = sub_idx.get(sid) or sub_idx.get(f"t3_{sid}")
        sub_feats_cache[sid] = _submission_features(sub)

    out_rows: List[Dict[str, Any]] = list(_iter_agg_rows(agg, sub_feats_cache))
    out = (
        pd.DataFrame(out_rows)
        .sort_values(["submission_id", "ticker"])
        .reset_index(drop=True)
    )
    return out