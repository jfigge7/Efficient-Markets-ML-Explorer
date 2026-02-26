import os
import re
import math
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union
from pathlib import Path

import pandas as pd
from transformers import pipeline

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
OUTPUT_ROWS_PER_FILE_DEFAULT = int(os.getenv("OBS_OUTPUT_ROWS_PER_FILE", "50"))
OUTPUT_FILE_PREFIX_DEFAULT = os.getenv("OBS_OUTPUT_PREFIX", "observations")

SORT_BEFORE_WRITE_DEFAULT = bool(os.getenv("SORT_BEFORE_WRITE", "False"))

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
_pipe_finbert = None
_pipe_twitter = None

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
        path = out_dir_p / f"{prefix}_{batch_idx:06d}.parquet"
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


# ----------------------------
# Model pipelines
# ----------------------------
def _is_local_path(p: str) -> bool:
    try:
        return Path(p).exists()
    except Exception:
        return False


def _pipeline_text_cls(model_ref: str, batch_size: int):
    local_only = _is_local_path(model_ref)
    return pipeline(
        "text-classification",
        model=model_ref,
        tokenizer=model_ref,
        device=-1,  # -1 = CUDA GPU, 0 = CPU  (note: your comment was reversed; leaving as-is)
        batch_size=batch_size,
        local_files_only=local_only,  # only enforce local if it's actually local
    )


def _get_finbert():
    global _pipe_finbert
    if _pipe_finbert is None:
        _pipe_finbert = _pipeline_text_cls(FINBERT_MODEL_DIR, BATCH_SIZE_FINBERT)
    return _pipe_finbert


def _get_twitter_roberta():
    global _pipe_twitter
    if _pipe_twitter is None:
        _pipe_twitter = _pipeline_text_cls(
            TWITTER_ROBERTA_MODEL_DIR, BATCH_SIZE_TWITTER
        )
    return _pipe_twitter


def _run_pipe_all_scores(pipe, texts: List[str]) -> List[List[Dict[str, Any]]]:
    out = pipe(texts, top_k=None, truncation=True)
    if isinstance(out, list) and out and isinstance(out[0], dict):
        return [out]
    return out


def _scores_to_fixed_schema(
    preds: Optional[List[Dict[str, Any]]],
    expected_labels: Tuple[str, str, str] = ("negative", "neutral", "positive"),
) -> Dict[str, float]:
    out = {lab: 0.0 for lab in expected_labels}
    for d in preds or []:
        lab = str(d.get("label", "")).strip().lower()
        sc = float(d.get("score", 0.0))

        if lab == "label_0":
            lab = "negative"
        elif lab == "label_1":
            lab = "neutral"
        elif lab == "label_2":
            lab = "positive"

        if lab in out:
            out[lab] = sc
    return out


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
def _make_submission_pseudo_rows(submissions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    pseudo: List[Dict[str, Any]] = []
    for s in submissions or []:
        sid_base = _base_id(s.get("id"))
        if not sid_base:
            continue

        author = s.get("author") if s.get("author") is not None else "[deleted]"
        title = _clean_text(s.get("title") or "")
        selftext = _clean_text(s.get("selftext") or "")

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


def _process_items_chunk(
    pipe_fin,
    pipe_tw,
    agg: Dict[Tuple[str, str], Dict[str, Any]],
    items: List[Dict[str, Any]],
):
    """
    items: list of dicts with keys:
      submission_id, ticker_text, author, comment_score, depth, weight, kind
    Runs both models on the chunk, extracts tickers, updates agg immediately.
    """
    if not items:
        return

    texts = [it["ticker_text"] for it in items]

    try:
        fin_preds = _run_pipe_all_scores(pipe_fin, texts)
    except Exception as e:
        print(f"Warning: finbert error on chunk (n={len(texts)}): {e}")
        fin_preds = [None] * len(texts)

    try:
        tw_preds = _run_pipe_all_scores(pipe_tw, texts)
    except Exception as e:
        print(f"Warning: twitter error on chunk (n={len(texts)}): {e}")
        tw_preds = [None] * len(texts)

    for it, fin_p, tw_p in zip(items, fin_preds, tw_preds):
        tickers = extract_tickers_from_text(it["ticker_text"])
        if not tickers:
            continue

        fin_scores = _scores_to_fixed_schema(
            fin_p, expected_labels=("negative", "neutral", "positive")
        )
        tw_scores = _scores_to_fixed_schema(
            tw_p, expected_labels=("negative", "neutral", "positive")
        )

        for tkr in tickers:
            _agg_update(
                agg=agg,
                submission_id=it["submission_id"],
                ticker=str(tkr).upper(),
                author=it["author"],
                comment_score=float(it["comment_score"]),
                depth=int(it["depth"]),
                weight=float(it["weight"]),
                kind=it["kind"],
                fin_scores=fin_scores,
                tw_scores=tw_scores,
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


def _write_agg_to_batched_files(
    agg: Dict[Tuple[str, str], Dict[str, Any]],
    sub_feats_cache: Dict[str, Dict[str, Any]],
    out_dir: Union[str, Path],
    rows_per_file: int = OUTPUT_ROWS_PER_FILE_DEFAULT,
    fmt: str = OUTPUT_FORMAT_DEFAULT,
    prefix: str = OUTPUT_FILE_PREFIX_DEFAULT,
    sort_before_write: bool = SORT_BEFORE_WRITE_DEFAULT,
) -> List[Path]:
    """
    Convert agg -> rows and write to disk in smaller dataframe batches.

    If sort_before_write=True, we sort keys first for deterministic order, but that
    requires materializing the sorted key list (still much smaller than a giant DF).
    """
    paths: List[Path] = []
    batch_rows: List[Dict[str, Any]] = []
    batch_idx = 0

    if sort_before_write:
        items_iter = (
            ((sid, tkr), agg[(sid, tkr)])
            for (sid, tkr) in sorted(agg.keys(), key=lambda x: (x[0], x[1]))
        )

        def row_iter():
            for (submission_id, ticker), a in items_iter:
                # inline copy of _iter_agg_rows logic to avoid extra dict iteration
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
                    "fin_neg_sum_w": float(a["fin_neg_wsum"]),
                    "fin_neu_sum_w": float(a["fin_neu_wsum"]),
                    "fin_pos_sum_w": float(a["fin_pos_wsum"]),
                    "tw_neg_sum_w": float(a["tw_neg_wsum"]),
                    "tw_neu_sum_w": float(a["tw_neu_wsum"]),
                    "tw_pos_sum_w": float(a["tw_pos_wsum"]),
                    "fin_neg_mean_w": float(a["fin_neg_wsum"]) / denom if weight_sum else float("nan"),
                    "fin_neu_mean_w": float(a["fin_neu_wsum"]) / denom if weight_sum else float("nan"),
                    "fin_pos_mean_w": float(a["fin_pos_wsum"]) / denom if weight_sum else float("nan"),
                    "tw_neg_mean_w": float(a["tw_neg_wsum"]) / denom if weight_sum else float("nan"),
                    "tw_neu_mean_w": float(a["tw_neu_wsum"]) / denom if weight_sum else float("nan"),
                    "tw_pos_mean_w": float(a["tw_pos_wsum"]) / denom if weight_sum else float("nan"),
                }
                yield row

        rows_iterable = row_iter()
    else:
        rows_iterable = _iter_agg_rows(agg, sub_feats_cache)

    for row in rows_iterable:
        batch_rows.append(row)
        if len(batch_rows) >= int(rows_per_file):
            path = _write_batch_rows(batch_rows, out_dir=out_dir, batch_idx=batch_idx, fmt=fmt, prefix=prefix)
            paths.append(path)
            batch_rows.clear()
            batch_idx += 1

    if batch_rows:
        path = _write_batch_rows(batch_rows, out_dir=out_dir, batch_idx=batch_idx, fmt=fmt, prefix=prefix)
        paths.append(path)
        batch_rows.clear()

    return paths


# ----------------------------
# Main: now supports batched output to disk
# ----------------------------
def build_observations_batched(
    comments: List[Dict[str, Any]],
    submissions: Optional[List[Dict[str, Any]]] = None,
    *,
    out_dir: Union[str, Path] = OUTPUT_DIR_DEFAULT,
    rows_per_file: int = OUTPUT_ROWS_PER_FILE_DEFAULT,
    file_format: str = OUTPUT_FORMAT_DEFAULT,  # parquet|csv
    file_prefix: str = OUTPUT_FILE_PREFIX_DEFAULT,
    sort_before_write: bool = SORT_BEFORE_WRITE_DEFAULT,
) -> List[Path]:
    """
    Like build_observations_df(), but instead of returning one giant dataframe,
    it writes smaller dataframe batches to out_dir and returns the written paths.

    This addresses the "ballooning dataframe" part by never materializing the full DF.

    Note: the aggregation dict (agg) can still grow large if you have an enormous number
    of (submission_id, ticker) pairs. If that becomes the bottleneck, the next step is
    spilling/merging aggregation itself.
    """
    if not isinstance(comments, list):
        raise ValueError("comments must be a list of dicts (depth computation needs full pass)")

    submissions = submissions or []

    _load_stocks()
    pipe_fin = _get_finbert()
    pipe_tw = _get_twitter_roberta()

    sub_idx = _index_submissions(submissions)
    depths_by_base = compute_depths(comments)

    agg: Dict[Tuple[str, str], Dict[str, Any]] = {}

    # -------------------------
    # Stream comments in chunks
    # -------------------------
    late_dropped = 0
    chunk_items: List[Dict[str, Any]] = []

    for c in comments:
        submission_id = _base_id(c.get("link_id"))  # link_id is t3_xxx
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
            _process_items_chunk(pipe_fin, pipe_tw, agg, chunk_items)
            chunk_items = []

    if chunk_items:
        _process_items_chunk(pipe_fin, pipe_tw, agg, chunk_items)

    if late_dropped:
        print(f"Dropped {late_dropped} late comments (> {COMMENT_CUTOFF_HOURS}h after submission).")

    # -------------------------
    # Process pseudo-items (titles/selftext) in chunks too
    # -------------------------
    pseudo_items = _make_submission_pseudo_rows(submissions)
    for pseudo_chunk in _chunks(pseudo_items, CHUNK_SIZE_ITEMS):
        _process_items_chunk(pipe_fin, pipe_tw, agg, pseudo_chunk)

    if not agg:
        return []

    # -------------------------
    # Submission features cache
    # -------------------------
    sub_feats_cache: Dict[str, Dict[str, Any]] = {}
    sids = sorted({sid for (sid, _tkr) in agg.keys()})
    for sid in sids:
        sub = sub_idx.get(sid) or sub_idx.get(f"t3_{sid}")
        sub_feats_cache[sid] = _submission_features(sub)

    # -------------------------
    # Write batches + return paths
    # -------------------------
    written_paths = _write_agg_to_batched_files(
        agg=agg,
        sub_feats_cache=sub_feats_cache,
        out_dir=out_dir,
        rows_per_file=rows_per_file,
        fmt=file_format,
        prefix=file_prefix,
        sort_before_write=sort_before_write,
    )
    return written_paths


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
    pipe_fin = _get_finbert()
    pipe_tw = _get_twitter_roberta()

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
            _process_items_chunk(pipe_fin, pipe_tw, agg, chunk_items)
            chunk_items = []

    if chunk_items:
        _process_items_chunk(pipe_fin, pipe_tw, agg, chunk_items)

    if late_dropped:
        print(f"Dropped {late_dropped} late comments (> {COMMENT_CUTOFF_HOURS}h after submission).")

    pseudo_items = _make_submission_pseudo_rows(submissions)
    for pseudo_chunk in _chunks(pseudo_items, CHUNK_SIZE_ITEMS):
        _process_items_chunk(pipe_fin, pipe_tw, agg, pseudo_chunk)

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


# ----------------------------
# Local test
# ----------------------------
if __name__ == "__main__":
    submissions = [
        {
            "id": "10s0uy",
            "author": "goodVentures",
            "title": "Thoughts on far OTM GOOG puts for earnings.",
            "selftext": "If they miss earnings, the stock is going to drop hard. Google could tank.",
            "score": 5,
            "upvote_ratio": 0.90,
            "num_comments": 10,
            "created_ts": "2012-10-01 13:22:10-07:00",
        }
    ]

    comments = [
        {
            "id": "c6g18xe",
            "author": "monkeycode",
            "body": "I think GOOG might surprise. $GOOG",
            "parent_id": "t3_10s0uy",
            "link_id": "t3_10s0uy",
            "score": 1,
            "created_ts": "2012-10-01 13:53:32-07:00",
        },
        {
            "id": "c6g18xf",
            "author": "replyguy",
            "body": "Agree — Google guidance matters.",
            "parent_id": "t1_c6g18xe",
            "link_id": "t3_10s0uy",
            "score": 2,
            "created_ts": "2012-10-01 15:00:00-07:00",
        },
        {
            "id": "c6g18xg",
            "author": "lateperson",
            "body": "Coming back days later: GOOG did fine.",
            "parent_id": "t3_10s0uy",
            "link_id": "t3_10s0uy",
            "score": 3,
            "created_ts": "2012-10-03 14:00:00-07:00",
        },
    ]

    print("Running build_observations_batched() local test...")
    paths = build_observations_batched(
        comments,
        submissions=submissions,
        out_dir=OUTPUT_DIR_DEFAULT,
        rows_per_file=1,          # force multiple tiny files for the demo
        file_format=OUTPUT_FORMAT_DEFAULT,        # csv avoids parquet engine dependency during a quick test
        file_prefix="obs_test",
    )
    print("Wrote batches:")
    for p in paths:
        print(" -", p)

    # If you still want to see the combined DF for the test:
    dfs = [pd.read_parquet(p) for p in paths]
    obs = pd.concat(dfs, ignore_index=True).sort_values(["submission_id", "ticker"]).reset_index(drop=True)

    print("\nObservations DF (reloaded from batches):")
    print(obs)

    assert (obs["submission_id"] == "10s0uy").all(), "submission_id mismatch"
    assert (obs["ticker"] == "GOOG").any(), "Expected GOOG ticker row"
    goog = obs[obs["ticker"] == "GOOG"].iloc[0]
    assert int(goog["n_from_comments"]) == 2, f"Expected 2 kept comments, got {goog['n_from_comments']}"

    print("\nLocal test passed (late comment filtered, pseudo-items included, batched writes OK).")