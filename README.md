# Shopping Sessions

A DuckDB pipeline for the [Builders buildathon](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions) shopping track. An online shop logs page views, add-to-carts, purchases, and ad clicks as dirty CSV. One shopper can show up under many user IDs. The job is to clean the logs, join those IDs, build sessions, credit each session to the ad click that brought the shopper in, and write two exact daily Parquet tables.

The score is container run time. A table that is almost right scores nothing, so the rules in `rules.md` are followed exactly.

## What the pipeline does

`pipeline/pipeline.py` runs inside the scorer's container. DuckDB does the scans, joins, session windows, and both daily aggregates.

1. Read every CSV by splitting on `\n` only. A `\r` stays inside the field, quoted fields are unwrapped, and a malformed line is dropped.
2. Deduplicate events on `event_id` before validation, keeping the earliest ingest time. Deduplicate clicks on `click_id`.
3. Keep an event only when its timestamp is inside the window, its type is `view`, `cart`, or `purchase`, and it was not ingested more than 24 hours late. Purchases also need a whole-cent price from 1 to 10,000,000.
4. Link user IDs into one person. The person's name is the byte-wise smallest ID in the component.
5. Start a new session when the gap between two of that person's events is over 30 minutes. The session's day is the UTC date of its first event.
6. Credit the session to the latest click in the previous 7 days. Ties go to the smallest `click_id`. A session with no qualifying click is `organic`. Every other campaign that clicked in that same window assists the session once.
7. Write `/output/user_daily/` (one row per person and day) and `/output/campaign_daily/` (one row per campaign and day).

## Checks

The 36 edge-case fixtures under `fixtures/` pass, and the public 1 million-event dev set matches the published answer exactly (117,038 shopper-day rows and 27,400 campaign-day rows).

```sh
pip install duckdb==1.5.6 polars
python scripts/run_fixtures.py fixtures -- python pipeline/pipeline.py
```

The public 1 million-event dev set is produced with the track's Rust generator (`generator.zip` on the track page, Rust 1.85 or newer) and checked against `dev-answer.zip`:

```sh
cargo build --release
target/release/generate --seed 1 --events 1000000 --clicks 250000 --output dev
# unzip dev-answer.zip so dev-answer/user_daily.csv and campaign_daily.csv are present
DATA_DIR=dev OUTPUT_DIR=out python pipeline/pipeline.py
python scripts/compare.py out dev-answer
```

Docker, which is how the scorer runs it:

```sh
make build
make fixtures
make dev          # needs generate on PATH
make run-dev
make check-dev
```

## Layout

| Path | Role |
| --- | --- |
| `pipeline/pipeline.py` | The pipeline |
| `Dockerfile`, `run.sh` | `linux/amd64` image. `CMD` takes no arguments |
| `rules.md` | The spec. It wins if a comment here disagrees with it |
| `fixtures/` | 36 edge cases and their expected tables |
| `scripts/` | Fixture runner and the exact output diff |

The generated dataset is not in this repository. The scorer mounts it at `/data`.
