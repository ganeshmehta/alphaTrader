"""
Redis metrics writer for Wikipedia Recent Changes pipeline.

Handles three metrics:
1. Edit velocity — edits per minute (last 10 min) using Sorted Set
2. Top editors — top 10 editors per wiki (last 10 min) using Sorted Set
3. Daily counts — edit/new counts per wiki for past 24 hours using Hash

Called from consumer.py on each micro-batch.
Also has a standalone function to backfill daily counts from Postgres.
"""

import os
import time
import redis
import psycopg2
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

# --- Redis config ---
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT"))
REDIS_DB = int(os.getenv("REDIS_DB"))

# --- Postgres config ---
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# --- Key patterns ---
# edit_velocity:{wiki} → Sorted Set (score=timestamp, member=meta_id)
# top_editors:{wiki}   → Sorted Set (score=edit_count, member=username)
# daily_counts:{wiki}  → Hash (field=YYYY-MM-DD-HH, value=count)

VELOCITY_WINDOW_SECONDS = 600   # 10 minutes
DAILY_WINDOW_HOURS = 24


def get_redis_client() -> redis.Redis:
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)


def update_metrics_from_batch(rows: list[dict]):
    """
    Called from consumer.py foreachBatch with list of row dicts.
    Updates Redis in-memory metrics for each row.

    Each row dict must have:
        wiki, change_type, user, meta_id, event_timestamp (epoch seconds float)
    """
    r = get_redis_client()
    now = time.time()
    cutoff = now - VELOCITY_WINDOW_SECONDS

    pipe = r.pipeline()

    for row in rows:
        wiki = row.get("wiki") or "unknown"
        user = row.get("user") or "anonymous"
        meta_id = row.get("meta_id") or str(now)
        ts = row.get("event_timestamp_epoch") or now
        hour_key = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d-%H")

        # 1. Edit velocity — add event to sorted set, score = timestamp
        velocity_key = f"edit_velocity:{wiki}"
        pipe.zadd(velocity_key, {meta_id: ts})
        pipe.zremrangebyscore(velocity_key, "-inf", cutoff)  # remove old entries
        pipe.expire(velocity_key, VELOCITY_WINDOW_SECONDS + 60)  # auto-expire key

        # 2. Top editors — increment editor score in sorted set
        editors_key = f"top_editors:{wiki}"
        pipe.zincrby(editors_key, 1, user)
        pipe.expire(editors_key, VELOCITY_WINDOW_SECONDS + 60)

        # 3. Daily counts — increment hour bucket in hash
        daily_key = f"daily_counts:{wiki}"
        pipe.hincrby(daily_key, hour_key, 1)
        pipe.expire(daily_key, DAILY_WINDOW_HOURS * 3600 + 3600)

    pipe.execute()


def get_edit_velocity(wiki: str) -> int:
    """Returns number of edits in the last 10 minutes for a wiki."""
    r = get_redis_client()
    cutoff = time.time() - VELOCITY_WINDOW_SECONDS
    return r.zcount(f"edit_velocity:{wiki}", cutoff, "+inf")


def get_top_editors(wiki: str, n: int = 10) -> list[tuple]:
    """Returns top N editors for a wiki in last 10 minutes."""
    r = get_redis_client()
    return r.zrevrange(f"top_editors:{wiki}", 0, n - 1, withscores=True)


def get_daily_counts(wiki: str) -> dict:
    """Returns edit counts per hour for last 24 hours for a wiki."""
    r = get_redis_client()
    return r.hgetall(f"daily_counts:{wiki}")


def backfill_daily_counts_from_postgres():
    """
    Reads last 24 hours of data from Postgres wiki_changes_bronze
    and populates Redis daily_counts hashes.
    Run this once on startup to backfill historical data.
    """
    r = get_redis_client()
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            wiki,
            to_char(event_timestamp AT TIME ZONE 'UTC', 'YYYY-MM-DD-HH24') AS hour_bucket,
            COUNT(*) AS edit_count
        FROM wiki_changes_bronze
        WHERE event_timestamp >= NOW() - INTERVAL '24 hours'
        GROUP BY wiki, hour_bucket
        ORDER BY wiki, hour_bucket
    """)

    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    pipe = r.pipeline()
    for wiki, hour_bucket, count in rows:
        daily_key = f"daily_counts:{wiki}"
        pipe.hset(daily_key, hour_bucket, count)
        pipe.expire(daily_key, DAILY_WINDOW_HOURS * 3600 + 3600)
    pipe.execute()

    print(f"Backfilled {len(rows)} hour buckets from Postgres into Redis")


if __name__ == "__main__":
    backfill_daily_counts_from_postgres()

    # Quick verification
    r = get_redis_client()
    print("\n--- Redis Metrics Snapshot ---")
    for wiki in ["enwiki", "commonswiki", "ruwiki"]:
        velocity = get_edit_velocity(wiki)
        top = get_top_editors(wiki, 3)
        daily = get_daily_counts(wiki)
        print(f"\n{wiki}:")
        print(f"  Edit velocity (last 10 min): {velocity}")
        print(f"  Top 3 editors: {top}")
        print(f"  Daily counts (last 24h buckets): {len(daily)} hours tracked")