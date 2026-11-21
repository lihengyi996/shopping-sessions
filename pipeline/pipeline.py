#!/usr/bin/env python3
"""Shopping Sessions pipeline (DuckDB).

Reads $DATA_DIR and writes $OUTPUT_DIR/user_daily and campaign_daily as Parquet.
The CSV dialect in rules.md is not what DuckDB's reader implements (\r is data,
not a line ending), so files are split on \\n from read_blob. Parsing, sessions,
attribution, and both aggregates run in DuckDB.
"""
import json
import os
import sys
from pathlib import Path

import duckdb

DATA = os.environ.get("DATA_DIR", "/data")
OUT = os.environ.get("OUTPUT_DIR", "/output")
SCRATCH = os.environ.get("SCRATCH_DIR") or os.path.join(
    os.environ.get("TMPDIR", os.environ.get("TEMP", "/tmp")), "shopping-scratch"
)
NCPU = int(os.environ.get("NCPU", os.cpu_count() or 1))
MEM_GB = int(os.environ.get("MEM_GB", "8"))

SEVEN_DAYS_MS = 7 * 24 * 60 * 60 * 1000
DAY_MS = 24 * 60 * 60 * 1000
SESSION_GAP_MS = 30 * 60 * 1000


def log(message):
    print(message, file=sys.stderr)


def qpath(path):
    return str(path).replace("\\", "/").replace("'", "''")


def csv_files(directory):
    folder = Path(directory)
    if not folder.is_dir():
        return []
    return sorted(p for p in folder.glob("*.csv") if p.is_file() and not p.name.startswith((".", "_")))


def connect():
    os.makedirs(os.path.join(SCRATCH, "duckdb_tmp"), exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET threads = {NCPU}")
    con.execute(f"SET memory_limit = '{max(1, int(MEM_GB * 0.8))}GB'")
    con.execute(f"SET temp_directory = '{qpath(os.path.join(SCRATCH, 'duckdb_tmp'))}'")
    con.execute("SET preserve_insertion_order = false")
    con.execute("SET TimeZone = 'UTC'")
    con.execute(
        r"""
        CREATE MACRO field_ok(f) AS (
          f IS NOT NULL AND (
            (
              length(f) >= 2
              AND starts_with(f, '"')
              AND ends_with(f, '"')
              AND length(f) - length(replace(f, '"', '')) = 2
            )
            OR NOT contains(f, '"')
          )
        );
        CREATE MACRO field_val(f) AS (
          nullif(
            CASE
              WHEN starts_with(f, '"') THEN substring(f, 2, length(f) - 2)
              ELSE f
            END,
            ''
          )
        );
        -- F1 UTC, F2 offset, F3 epoch milliseconds. NULL means a bad timestamp
        -- or an F3 that does not fit in int64 (outside every contest window).
        CREATE MACRO parse_ts(s) AS (
          CASE
            WHEN s IS NULL THEN NULL
            WHEN regexp_full_match(s, '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$')
              THEN epoch_ms(try_strptime(s, '%Y-%m-%dT%H:%M:%SZ'))
            WHEN regexp_full_match(s, '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[+-][0-9]{2}:[0-9]{2}$')
              THEN CASE
                WHEN try_cast(substr(s, 21, 2) AS INTEGER) > 23
                  OR try_cast(substr(s, 24, 2) AS INTEGER) > 59
                  THEN NULL
                ELSE epoch_ms(try_strptime(substr(s, 1, 19), '%Y-%m-%dT%H:%M:%S'))
                  - CASE WHEN substr(s, 20, 1) = '+' THEN 1 ELSE -1 END
                    * (
                      try_cast(substr(s, 21, 2) AS BIGINT) * 3600000
                      + try_cast(substr(s, 24, 2) AS BIGINT) * 60000
                    )
              END
            WHEN regexp_full_match(s, '^[0-9]+$') THEN CASE
              WHEN length(ltrim(s, '0')) > 19 THEN NULL
              WHEN length(ltrim(s, '0')) = 19 AND ltrim(s, '0') > '9223372036854775807' THEN NULL
              ELSE try_cast(s AS BIGINT)
            END
            ELSE NULL
          END
        );
        """
    )
    return con


def load_lines(con, paths, table, schema, column_sql, where_sql):
    """Skip each file's header, split on \\n only, keep rows that match the dialect."""
    con.execute(f"CREATE OR REPLACE TABLE {table} ({schema})")
    if not paths:
        return
    insert = f"""
        INSERT INTO {table}
        SELECT {column_sql}
        FROM (
          SELECT string_split(line, ',') AS p
          FROM (
            SELECT lines[i] AS line
            FROM (
              SELECT string_split(decode(content), chr(10)) AS lines
              FROM (SELECT content FROM read_blob(?))
            ) s,
            range(2, len(s.lines) + 1) r(i)
          )
          WHERE line <> ''
        )
        WHERE {where_sql}
    """
    for path in paths:
        con.execute(insert, [path.as_posix()])


def load_inputs(con):
    ok7 = " AND ".join(f"field_ok(p[{i}])" for i in range(1, 8))
    ok4 = " AND ".join(f"field_ok(p[{i}])" for i in range(1, 5))
    ok2 = "field_ok(p[1]) AND field_ok(p[2])"
    load_lines(
        con,
        csv_files(os.path.join(DATA, "events")),
        "event_raw",
        """
        event_id VARCHAR, user_id VARCHAR, ts_raw VARCHAR, ingest_raw VARCHAR,
        event_type VARCHAR, product_id VARCHAR, price_raw VARCHAR
        """,
        """
        field_val(p[1]) AS event_id,
        field_val(p[2]) AS user_id,
        field_val(p[3]) AS ts_raw,
        field_val(p[4]) AS ingest_raw,
        field_val(p[5]) AS event_type,
        field_val(p[6]) AS product_id,
        field_val(p[7]) AS price_raw
        """,
        f"len(p) = 7 AND {ok7} AND field_val(p[1]) IS NOT NULL",
    )
    load_lines(
        con,
        csv_files(os.path.join(DATA, "clicks")),
        "click_raw",
        "click_id VARCHAR, user_id VARCHAR, campaign_id VARCHAR, ts_raw VARCHAR",
        """
        field_val(p[1]) AS click_id,
        field_val(p[2]) AS user_id,
        field_val(p[3]) AS campaign_id,
        field_val(p[4]) AS ts_raw
        """,
        f"len(p) = 4 AND {ok4} AND field_val(p[1]) IS NOT NULL",
    )
    load_lines(
        con,
        csv_files(os.path.join(DATA, "links")),
        "links",
        "a VARCHAR, b VARCHAR",
        "field_val(p[1]) AS a, field_val(p[2]) AS b",
        f"len(p) = 2 AND {ok2} AND field_val(p[1]) IS NOT NULL AND field_val(p[2]) IS NOT NULL",
    )
    load_lines(
        con,
        [Path(os.path.join(DATA, "products.csv"))] if Path(os.path.join(DATA, "products.csv")).is_file() else [],
        "product_raw",
        "product_id VARCHAR, category VARCHAR",
        "field_val(p[1]) AS product_id, field_val(p[2]) AS category",
        f"len(p) = 2 AND {ok2} AND field_val(p[1]) IS NOT NULL AND field_val(p[2]) IS NOT NULL",
    )
    load_lines(
        con,
        [Path(os.path.join(DATA, "campaigns.csv"))] if Path(os.path.join(DATA, "campaigns.csv")).is_file() else [],
        "campaign_raw",
        "campaign_id VARCHAR, channel VARCHAR",
        "field_val(p[1]) AS campaign_id, field_val(p[2]) AS channel",
        f"len(p) = 2 AND {ok2} AND field_val(p[1]) IS NOT NULL AND field_val(p[2]) IS NOT NULL",
    )
    con.execute(
        """
        CREATE OR REPLACE TABLE products AS
        SELECT product_id, any_value(category) AS category
        FROM product_raw GROUP BY product_id;
        CREATE OR REPLACE TABLE campaigns AS
        SELECT campaign_id, any_value(channel) AS channel
        FROM campaign_raw GROUP BY campaign_id;
        """
    )


def link_people(con):
    """Person name is the byte-wise smallest id in a connected component."""
    pairs = con.execute("SELECT a, b FROM links").fetchall()
    parent = {}
    rank = {}
    label = {}

    def find(node):
        parent.setdefault(node, node)
        rank.setdefault(node, 0)
        label.setdefault(node, node)
        root = node
        while parent[root] != root:
            root = parent[root]
        while parent[node] != root:
            parent[node], node = root, parent[node]
        return root

    def union(left, right):
        ra, rb = find(left), find(right)
        if ra == rb:
            return
        if rank[ra] < rank[rb]:
            ra, rb = rb, ra
        parent[rb] = ra
        if rank[ra] == rank[rb]:
            rank[ra] += 1
        if label[rb] < label[ra]:
            label[ra] = label[rb]

    for left, right in pairs:
        union(left, right)

    ids = []
    people = []
    for node in list(parent):
        person = label[find(node)]
        if person != node:
            ids.append(node)
            people.append(person)

    con.execute("CREATE TABLE id_map(user_id VARCHAR, person VARCHAR)")
    step = 200_000
    for start in range(0, len(ids), step):
        con.execute(
            "INSERT INTO id_map SELECT * FROM (SELECT unnest(?) AS user_id, unnest(?) AS person)",
            [ids[start : start + step], people[start : start + step]],
        )


def transform(con, window_start, window_end):
    con.execute(
        "CREATE OR REPLACE TABLE bounds AS SELECT parse_ts(?) AS start_ms, parse_ts(?) AS end_ms",
        [window_start, window_end],
    )
    con.execute(
        f"""
        CREATE OR REPLACE TABLE events AS
        WITH dedup AS (
          SELECT * EXCLUDE (rn)
          FROM (
            SELECT
              event_id, user_id, event_type, product_id, price_raw,
              parse_ts(ts_raw) AS ts_ms,
              parse_ts(ingest_raw) AS ingest_ms,
              row_number() OVER (PARTITION BY event_id ORDER BY parse_ts(ingest_raw) ASC NULLS LAST) AS rn
            FROM event_raw
          )
          WHERE rn = 1
        ),
        kept AS (
          SELECT
            d.event_id,
            d.user_id,
            d.ts_ms,
            d.event_type,
            d.product_id,
            CASE
              WHEN d.event_type = 'purchase'
                AND regexp_full_match(d.price_raw, '^[0-9]+$')
                AND length(ltrim(d.price_raw, '0')) <= 8
                AND try_cast(d.price_raw AS BIGINT) BETWEEN 1 AND 10000000
              THEN try_cast(d.price_raw AS BIGINT)
              ELSE NULL
            END AS price
          FROM dedup d, bounds b
          WHERE d.user_id IS NOT NULL
            AND d.ts_ms IS NOT NULL
            AND d.ts_ms >= b.start_ms
            AND d.ts_ms < b.end_ms
            AND d.event_type IN ('view', 'cart', 'purchase')
            AND d.product_id IS NOT NULL
            AND d.ingest_ms <= d.ts_ms + {DAY_MS}
            AND (
              d.event_type <> 'purchase'
              OR (
                regexp_full_match(d.price_raw, '^[0-9]+$')
                AND length(ltrim(d.price_raw, '0')) <= 8
                AND try_cast(d.price_raw AS BIGINT) BETWEEN 1 AND 10000000
              )
            )
        )
        SELECT
          k.event_id,
          coalesce(m.person, k.user_id) AS person,
          k.ts_ms,
          k.event_type,
          coalesce(p.category, 'unknown') AS category,
          k.price
        FROM kept k
        LEFT JOIN id_map m ON k.user_id = m.user_id
        LEFT JOIN products p ON k.product_id = p.product_id;

        CREATE OR REPLACE TABLE clicks AS
        WITH dedup AS (
          SELECT * EXCLUDE (rn)
          FROM (
            SELECT
              click_id, user_id, campaign_id,
              parse_ts(ts_raw) AS ts_ms,
              row_number() OVER (PARTITION BY click_id) AS rn
            FROM click_raw
          )
          WHERE rn = 1
        )
        SELECT
          d.click_id,
          coalesce(m.person, d.user_id) AS person,
          d.campaign_id,
          c.channel,
          d.ts_ms
        FROM dedup d
        JOIN campaigns c ON d.campaign_id = c.campaign_id
        JOIN bounds b ON d.ts_ms >= b.start_ms - {SEVEN_DAYS_MS} AND d.ts_ms < b.end_ms
        LEFT JOIN id_map m ON d.user_id = m.user_id
        WHERE d.user_id IS NOT NULL AND d.ts_ms IS NOT NULL;

        CREATE OR REPLACE TABLE session_events AS
        WITH gaps AS (
          SELECT
            person,
            ts_ms,
            ts_ms - lag(ts_ms) OVER (PARTITION BY person ORDER BY ts_ms) AS gap
          FROM (SELECT DISTINCT person, ts_ms FROM events)
        ),
        numbered AS (
          SELECT
            person,
            ts_ms,
            sum(CASE WHEN gap IS NULL OR gap > {SESSION_GAP_MS} THEN 1 ELSE 0 END) OVER (
              PARTITION BY person
              ORDER BY ts_ms
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS sess_n
          FROM gaps
        ),
        starts AS (
          SELECT
            person,
            sess_n,
            min(ts_ms) AS session_start,
            DATE '1970-01-01' + CAST(min(ts_ms) // {DAY_MS} AS INTEGER) AS day
          FROM numbered
          GROUP BY person, sess_n
        )
        SELECT
          e.person,
          n.sess_n,
          s.session_start,
          s.day,
          e.event_type,
          e.category,
          e.price
        FROM events e
        JOIN numbered n ON e.person = n.person AND e.ts_ms = n.ts_ms
        JOIN starts s ON n.person = s.person AND n.sess_n = s.sess_n;

        CREATE OR REPLACE TABLE sessions AS
        SELECT
          person,
          sess_n,
          any_value(session_start) AS session_start,
          any_value(day) AS day,
          count(*) AS events,
          count(*) FILTER (WHERE event_type = 'purchase') AS purchases,
          coalesce(sum(price) FILTER (WHERE event_type = 'purchase'), 0) AS revenue
        FROM session_events
        GROUP BY person, sess_n;

        CREATE OR REPLACE TABLE credit AS
        SELECT person, sess_n, day, purchases, revenue, campaign_id, channel
        FROM (
          SELECT
            s.person,
            s.sess_n,
            s.day,
            s.purchases,
            s.revenue,
            c.campaign_id,
            c.channel,
            c.click_id,
            c.ts_ms
          FROM sessions s
          LEFT JOIN clicks c
            ON c.person = s.person
            AND c.ts_ms <= s.session_start
            AND s.session_start - c.ts_ms <= {SEVEN_DAYS_MS}
        )
        QUALIFY row_number() OVER (
          PARTITION BY person, sess_n
          ORDER BY ts_ms DESC NULLS LAST, click_id ASC NULLS LAST
        ) = 1;

        CREATE OR REPLACE TABLE assists AS
        SELECT DISTINCT
          s.person,
          s.sess_n,
          s.day,
          c.campaign_id,
          c.channel
        FROM sessions s
        JOIN clicks c
          ON c.person = s.person
          AND c.ts_ms <= s.session_start
          AND s.session_start - c.ts_ms <= {SEVEN_DAYS_MS};

        CREATE OR REPLACE TABLE top_category AS
        SELECT person, day, category AS top_category
        FROM (
          SELECT person, day, category, sum(price) AS revenue
          FROM session_events
          WHERE event_type = 'purchase'
          GROUP BY person, day, category
        )
        QUALIFY row_number() OVER (
          PARTITION BY person, day
          ORDER BY revenue DESC, category ASC
        ) = 1;

        CREATE OR REPLACE TABLE user_daily AS
        SELECT
          s.person AS user_id,
          s.day,
          count(*) AS sessions,
          sum(s.events) AS events,
          sum(s.revenue) AS revenue_cents,
          any_value(t.top_category) AS top_category
        FROM sessions s
        LEFT JOIN top_category t ON s.person = t.person AND s.day = t.day
        GROUP BY s.person, s.day;

        CREATE OR REPLACE TABLE credited_daily AS
        SELECT
          coalesce(campaign_id, 'organic') AS campaign_id,
          coalesce(channel, 'organic') AS channel,
          day,
          count(*) AS sessions,
          count(*) FILTER (WHERE purchases > 0) AS purchasing_sessions,
          sum(revenue) AS revenue_cents
        FROM credit
        GROUP BY 1, 2, 3;

        CREATE OR REPLACE TABLE assisted_daily AS
        SELECT campaign_id, channel, day, count(*) AS assisted_sessions
        FROM assists
        GROUP BY 1, 2, 3;

        CREATE OR REPLACE TABLE campaign_daily AS
        SELECT
          coalesce(c.campaign_id, a.campaign_id) AS campaign_id,
          coalesce(c.channel, a.channel) AS channel,
          coalesce(c.day, a.day) AS day,
          coalesce(c.sessions, 0) AS sessions,
          coalesce(c.purchasing_sessions, 0) AS purchasing_sessions,
          coalesce(c.revenue_cents, 0) AS revenue_cents,
          CASE
            WHEN coalesce(c.campaign_id, a.campaign_id) = 'organic' THEN 0
            ELSE coalesce(a.assisted_sessions, 0)
          END AS assisted_sessions
        FROM credited_daily c
        FULL OUTER JOIN assisted_daily a
          ON c.campaign_id = a.campaign_id AND c.day = a.day
        WHERE coalesce(c.sessions, 0) > 0 OR coalesce(a.assisted_sessions, 0) > 0;
        """
    )


def write_outputs(con):
    user_dir = os.path.join(OUT, "user_daily")
    camp_dir = os.path.join(OUT, "campaign_daily")
    os.makedirs(user_dir, exist_ok=True)
    os.makedirs(camp_dir, exist_ok=True)
    for folder in (user_dir, camp_dir):
        for old in Path(folder).glob("*.parquet"):
            if old.is_file():
                old.unlink()
    con.execute(
        f"""
        COPY (
          SELECT
            user_id::VARCHAR AS user_id,
            day::DATE AS day,
            sessions::BIGINT AS sessions,
            events::BIGINT AS events,
            revenue_cents::BIGINT AS revenue_cents,
            top_category::VARCHAR AS top_category
          FROM user_daily
        ) TO '{qpath(os.path.join(user_dir, "part-0.parquet"))}' (FORMAT parquet)
        """
    )
    con.execute(
        f"""
        COPY (
          SELECT
            campaign_id::VARCHAR AS campaign_id,
            channel::VARCHAR AS channel,
            day::DATE AS day,
            sessions::BIGINT AS sessions,
            purchasing_sessions::BIGINT AS purchasing_sessions,
            revenue_cents::BIGINT AS revenue_cents,
            assisted_sessions::BIGINT AS assisted_sessions
          FROM campaign_daily
        ) TO '{qpath(os.path.join(camp_dir, "part-0.parquet"))}' (FORMAT parquet)
        """
    )


def main():
    with open(os.path.join(DATA, "manifest.json"), encoding="utf-8") as handle:
        manifest = json.load(handle)
    con = connect()
    load_inputs(con)
    link_people(con)
    transform(con, manifest["window_start"], manifest["window_end"])
    write_outputs(con)
    log(
        "wrote user_daily and campaign_daily for "
        f"{manifest['window_start']} .. {manifest['window_end']}"
    )


if __name__ == "__main__":
    main()
