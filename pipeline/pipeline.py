#!/usr/bin/env python3
"""Run the Rust pipeline, then write the two Parquet tables DuckDB already knows."""
import os
import subprocess
import sys
import tempfile

import duckdb

DATA = os.environ.get("DATA_DIR", "/data")
OUT = os.environ.get("OUTPUT_DIR", "/output")
SCRATCH = os.environ.get("SCRATCH_DIR") or os.path.join(
    tempfile.gettempdir(), "shopping-scratch"
)
HERE = os.path.dirname(os.path.abspath(__file__))


def binary():
    override = os.environ.get("PIPELINE_BIN")
    if override:
        return override
    name = "shopsess.exe" if os.name == "nt" else "shopsess"
    candidates = [
        os.path.join(HERE, "..", "rust", "target", "release", name),
        os.path.join("/usr/local/bin", "shopsess"),
    ]
    for path in candidates:
        if os.path.isfile(path):
            return path
    sys.exit("shopsess binary not found; build rust/ first")


def sql_path(path):
    return path.replace("\\", "/").replace("'", "''")


def write_parquet(con, csv_path, select_sql, folder):
    os.makedirs(folder, exist_ok=True)
    for name in os.listdir(folder):
        if name.endswith(".parquet"):
            os.remove(os.path.join(folder, name))
    dest = sql_path(os.path.join(folder, "part-0.parquet"))
    src = sql_path(csv_path)
    con.execute(
        f"""
        COPY ( {select_sql.format(src=src)} )
        TO '{dest}' (FORMAT PARQUET)
        """
    )


def main():
    os.makedirs(SCRATCH, exist_ok=True)
    env = dict(os.environ, DATA_DIR=DATA, OUTPUT_DIR=OUT, SCRATCH_DIR=SCRATCH)
    subprocess.run([binary()], env=env, check=True)
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    write_parquet(
        con,
        os.path.join(SCRATCH, "user_daily.csv"),
        """
        SELECT user_id::VARCHAR AS user_id,
               CAST(day AS DATE) AS day,
               sessions::BIGINT AS sessions,
               events::BIGINT AS events,
               revenue_cents::BIGINT AS revenue_cents,
               NULLIF(top_category, '')::VARCHAR AS top_category
        FROM read_csv('{src}', header=true, quote='"', columns={{
            'user_id': 'VARCHAR', 'day': 'VARCHAR', 'sessions': 'BIGINT',
            'events': 'BIGINT', 'revenue_cents': 'BIGINT', 'top_category': 'VARCHAR'
        }})
        """,
        os.path.join(OUT, "user_daily"),
    )
    write_parquet(
        con,
        os.path.join(SCRATCH, "campaign_daily.csv"),
        """
        SELECT campaign_id::VARCHAR AS campaign_id,
               channel::VARCHAR AS channel,
               CAST(day AS DATE) AS day,
               sessions::BIGINT AS sessions,
               purchasing_sessions::BIGINT AS purchasing_sessions,
               revenue_cents::BIGINT AS revenue_cents,
               assisted_sessions::BIGINT AS assisted_sessions
        FROM read_csv('{src}', header=true, quote='"', columns={{
            'campaign_id': 'VARCHAR', 'channel': 'VARCHAR', 'day': 'VARCHAR',
            'sessions': 'BIGINT', 'purchasing_sessions': 'BIGINT',
            'revenue_cents': 'BIGINT', 'assisted_sessions': 'BIGINT'
        }})
        """,
        os.path.join(OUT, "campaign_daily"),
    )


if __name__ == "__main__":
    main()
