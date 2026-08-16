# Real-Time Wikipedia Recent-Changes Pipeline

Streams live edits from Wikimedia's public EventStreams feed into Kafka, then
into Postgres via PySpark Structured Streaming.

## Architecture (current scope)

```
Wikimedia EventStreams (SSE)
        ↓
producer/producer.py  →  Kafka topic: raw_wiki_changes
        ↓
spark_consumer/consumer.py  (PySpark Structured Streaming)
        ↓
PostgreSQL: wiki_changes_bronze
```

Airflow and dbt are intentionally out of scope for now — this repo currently
covers ingestion through to a raw (Bronze) Postgres table. Docker packaging
is also on hold; everything currently runs against a locally installed Kafka
and Postgres.

## Components

- **`producer/`** — connects to `https://stream.wikimedia.org/v2/stream/recentchange`,
  filters by wiki / event type / bot status, and publishes normalized JSON
  events to a Kafka topic.
- **`spark_consumer/`** — Structured Streaming job that reads that topic,
  parses and reshapes the events, and appends them to Postgres in 10s
  micro-batches, deduped by the source event's `meta_id`.
- **`postgres_init/01_bronze_schema.sql`** — creates the `wiki_changes_bronze`
  landing table.

## Running locally (no Docker yet)

1. Create the Bronze table:
   ```bash
   psql -U <user> -d <db> -f postgres_init/01_bronze_schema.sql
   ```

2. Start the producer:
   ```bash
   cd producer
   pip install -r requirements.txt
   python producer.py
   ```
   Configurable via env vars: `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`,
   `WIKI_FILTER`, `TYPE_FILTER`, `EXCLUDE_BOTS`.

3. Start the consumer:
   ```bash
   cd spark_consumer
   pip install -r requirements.txt
   spark-submit \
     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
     consumer.py
   ```
   Configurable via env vars: `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`,
   `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`,
   `POSTGRES_PASSWORD`, `POSTGRES_TABLE`.

## Roadmap

- [ ] Validate end-to-end locally
- [ ] Silver/Gold transformations
- [ ] Docker packaging
- [ ] Orchestration (Airflow) — later
