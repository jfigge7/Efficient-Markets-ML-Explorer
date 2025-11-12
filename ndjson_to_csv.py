#!/usr/bin/env python3
"""
ndjson_to_csv.py

Stream a massive NDJSON (newline-delimited JSON) file (optionally .zst or .gz)
and write selected fields to CSV without loading the whole file into memory.

Examples:
  python ndjson_to_csv.py -i "C:\data\wallstreetbets_comments" -o "C:\data\wsb_comments.csv" \
      --fields id author body score created_utc --as-datetime created_utc

  python ndjson_to_csv.py -i "C:\data\wsb_comments.json.zst" -o "C:\data\wsb_comments.csv" \
      --fields id author body score created_utc --contains "GME" --min-utc 1609459200 --max-utc 1640995200

Notes:
- Supports plain .ndjson/.json, .gz (gzip), and .zst (zstandard) inputs.
- For .zst input, `pip install zstandard` is required.
- Use --max-rows-per-file to split giant outputs (e.g., 5_000_000 rows per CSV).
"""

from __future__ import annotations
import argparse
import csv
import io
import json
import os
import sys
import gzip
from typing import Iterable, Optional, List

def open_any(path: str) -> Iterable[str]:
    """
    Open a text stream for the given path.
    Supports:
      - plain text (default)
      - .gz via gzip
      - .zst via zstandard (optional dependency)
    Yields decoded lines as str.
    """
    lower = path.lower()
    if lower.endswith(".gz"):
        f = gzip.open(path, "rt", encoding="utf-8", errors="replace", newline="")
        return f

    if lower.endswith(".zst"):
        try:
            import zstandard as zstd  # type: ignore
        except Exception as e:
            print("ERROR: .zst file detected but the 'zstandard' package is not installed.", file=sys.stderr)
            print("Install it with:  pip install zstandard", file=sys.stderr)
            raise
        raw = open(path, "rb")
        dctx = zstd.ZstdDecompressor()
        reader = dctx.stream_reader(raw)
        # Wrap the binary reader as text
        text_stream = io.TextIOWrapper(reader, encoding="utf-8", errors="replace", newline="")
        return text_stream

    # Plain text
    return open(path, "rt", encoding="utf-8", errors="replace", newline="")

def write_csv_row(writer: csv.writer, fields: List[str], obj: dict, datetime_fields: List[str]):
    row = []
    for f in fields:
        val = obj.get(f, None)
        if val is None:
            row.append("")
            continue
        if f in datetime_fields:
            # created_utc-style numeric timestamp to ISO8601 (UTC)
            try:
                import datetime as _dt
                # Handle both seconds and milliseconds
                ts = float(val)
                if ts > 10_000_000_000:  # likely ms
                    ts = ts / 1000.0
                iso = _dt.datetime.utcfromtimestamp(ts).strftime("%Y-%m-%dT%H:%M:%SZ")
                row.append(iso)
            except Exception:
                row.append(str(val))
        else:
            if isinstance(val, (dict, list)):
                row.append(json.dumps(val, ensure_ascii=False))
            else:
                row.append(str(val))
    writer.writerow(row)

def main():
    p = argparse.ArgumentParser(description="Stream NDJSON (optionally .zst/.gz) and extract fields to CSV.")
    p.add_argument("-i", "--input", required=True, help="Path to NDJSON (.json/.ndjson/.gz/.zst)")
    p.add_argument("-o", "--output", required=True, help="Path to output CSV (or prefix if splitting)")
    p.add_argument("--fields", nargs="+", default=["id", "author", "body", "score", "created_utc"],
                   help="Fields to extract (space-separated)")
    p.add_argument("--as-datetime", nargs="*", default=[],
                   help="Field names that should be converted from unix epoch to ISO8601 (e.g., created_utc)")
    p.add_argument("--contains", nargs="*", default=[],
                   help="Only include rows where *any* of these strings appear (case-insensitive) in the 'body' or 'title'")
    p.add_argument("--min-utc", type=float, default=None,
                   help="Only include objects with created_utc >= this value (seconds since epoch)")
    p.add_argument("--max-utc", type=float, default=None,
                   help="Only include objects with created_utc <= this value (seconds since epoch)")
    p.add_argument("--created-field", default="created_utc",
                   help="Name of the timestamp field (default: created_utc)")
    p.add_argument("--max-rows-per-file", type=int, default=0,
                   help="If >0, split output into multiple CSVs with this many data rows each.")
    args = p.parse_args()

    inp = args.input
    out = args.output
    fields = args.fields
    contains = [s.lower() for s in (args.contains or [])]
    min_utc = args.min_utc
    max_utc = args.max_utc
    created_field = args.created_field
    
    # Fix arg name for Python variable
    if hasattr(args, "as_datetime"):
        datetime_fields = list(set(args.as_datetime))
    else:
        datetime_fields = []

    # Prepare output writer(s)
    def make_writer(idx: Optional[int] = None):
        if idx is None:
            csv_path = out
        else:
            base, ext = os.path.splitext(out)
            if not ext:
                ext = ".csv"
            csv_path = f"{base}_part{idx:04d}{ext}"
        f = open(csv_path, "w", encoding="utf-8", newline="")
        w = csv.writer(f)
        w.writerow(fields)
        return f, w, csv_path

    out_file, writer, current_path = make_writer(None if args.max_rows_per_file <= 0 else 1)
    rows_in_current = 0
    part_idx = 1 if args.max_rows_per_file > 0 else None

    total = 0
    kept = 0

    try:
        with open_any(inp) as fin:
            for line in fin:
                total += 1
                if not line.strip():
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    # Skip bad JSON line
                    continue

                # Filters
                if contains:
                    text = ((obj.get("body") or "") + " " + (obj.get("title") or "")).lower()
                    if not any(term in text for term in contains):
                        continue

                if min_utc is not None or max_utc is not None:
                    ts = obj.get(created_field)
                    try:
                        ts = float(ts)
                    except Exception:
                        ts = None
                    if ts is None:
                        continue
                    if min_utc is not None and ts < min_utc:
                        continue
                    if max_utc is not None and ts > max_utc:
                        continue

                # Write row
                write_csv_row(writer, fields, obj, datetime_fields)
                kept += 1
                rows_in_current += 1

                # Rotate if splitting
                if args.max_rows_per_file > 0 and rows_in_current >= args.max_rows_per_file:
                    out_file.close()
                    part_idx = (part_idx or 0) + 1
                    out_file, writer, current_path = make_writer(part_idx)
                    rows_in_current = 0
    finally:
        try:
            out_file.close()
        except Exception:
            pass

    print(f"Done. Read {total:,} lines; wrote {kept:,} rows to CSV(s).")
    if part_idx:
        base, ext = os.path.splitext(out)
        if not ext: ext = ".csv"
        print(f"Output files look like: {base}_part0001{ext}, {base}_part0002{ext}, ...")
    else:
        print(f"Output CSV: {current_path}")
        
if __name__ == "__main__":
    main()
