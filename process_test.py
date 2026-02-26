from preprocessing import build_observations_batched as new
#from preprocessing_old import build_observations_batched as outdated
import os
from dotenv import load_dotenv
load_dotenv()
import duckdb
from pathlib import Path
from typing import List, Dict, Any
import pandas as pd
con = duckdb.connect()

COMMENTS = str(Path(os.getenv("DATA_DIR")) / "comment_db" / "year=2012" / "month=4" / "day=*" / "*.parquet")
SUBMISSIONS = str(Path(os.getenv("DATA_DIR")) / "submission_db" / "year=2012" / "month=4" / "day=*" / "*.parquet")
comments_df = con.execute(
    f"SELECT * FROM read_parquet('{COMMENTS}')"
).df()

submissions_df = con.execute(
    f"SELECT * FROM read_parquet('{SUBMISSIONS}')"
).df()

comments: List[Dict[str, Any]] = comments_df.to_dict(orient="records")
submissions: List[Dict[str, Any]] = submissions_df.to_dict(orient="records")

df1 = new(comments, submissions)
dfs1 = [pd.read_parquet(p) for p in df1]
obs1 = pd.concat(dfs1, ignore_index=True).sort_values(by=["submission_id", "ticker"]).reset_index(drop=True)
print(obs1)
#df2 = outdated(comments, submissions)
#dfs2 = [pd.read_parquet(p) for p in df2]
#obs2 = pd.concat(dfs2, ignore_index=True).sort_values(by=["submission_id", "ticker"]).reset_index(drop=True)
#merged_df = pd.merge(obs1, obs2, on=['submission_id', 'ticker'], how='outer', indicator=True)

# Filter for rows that are not in both DataFrames
#differences = merged_df[merged_df['_merge'] != 'both']
#print(differences)
#print(len(obs1), len(obs2), len(differences))
#print(obs1.items())