# Real-Time Wikipedia Recent-Changes Pipeline

Streams live edits from Wikimedia's public EventStreams feed into Kafka, then
into Postgres via PySpark Structured Streaming.

## Architecture (current scope)

```
Wikimedia EventStreams (SSE)
        ↓
producer.py  →  Kafka topic: raw_wiki_changes
        ↓
consumer.py  (PySpark Structured Streaming)
        ↓
PostgreSQL: wiki_changes_bronze
```

Airflow and dbt are intentionally out of scope for now. Docker packaging
is also on hold; everything currently runs against a locally installed Kafka
and Postgres.

## Components

- **`producer.py`** — connects to `https://stream.wikimedia.org/v2/stream/recentchange`,
  filters by wiki / event type / bot status, and publishes normalized JSON
  events to a Kafka topic.
- **`consumer.py`** — Structured Streaming job that reads that topic,
  parses and reshapes the events, and appends them to Postgres in 10s
  micro-batches, deduped by the source event's `meta_id`.
- **`postgres_init/01_bronze_schema.sql`** — creates the `wiki_changes_bronze`
  landing table.

## Prerequisites

- Java 17 (JDK) — required for Kafka and Spark
- Apache Kafka 3.8.0 — installed at `C:\kafka`
- PostgreSQL 16 — running locally
- Python 3.x with virtualenv
- Hadoop winutils — installed at `C:\hadoop\bin` (required for Spark on Windows)

## Setup

1. Create the Bronze table in pgAdmin or psql:
   ```sql
   -- run postgres_init/01_bronze_schema.sql against your database
   ```

2. Create and configure `.env` file in project root:
   ```
   KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092
   WIKI_STREAM_URL=https://stream.wikimedia.org/v2/stream/recentchange
   WIKI_FILTER=enwiki
   TYPE_FILTER=edit,new
   EXCLUDE_BOTS=true
   POSTGRES_HOST=127.0.0.1
   POSTGRES_PORT=5432
   POSTGRES_DB=your_db_name
   POSTGRES_USER=your_username
   POSTGRES_PASSWORD=your_password
   POSTGRES_TABLE=wiki_changes_bronze
   CHECKPOINT_LOCATION=./_checkpoints/wiki_changes_bronze
   ```

3. Install dependencies:
   ```powershell
   pip install -r requirements.txt
   ```

## Running locally (Windows)

### Step 1 — Start infrastructure (Zookeeper + Kafka)
```powershell
.\infra_start.bat
```
Starts Zookeeper and Kafka broker. Run this once before starting producer or consumer.

### Step 2 — Create Kafka topic
```powershell
.\create_topic.bat raw_wiki_changes 1 1
```
Usage: `create_topic.bat <topic_name> <partitions> <replication_factor>`

### Step 3 — Start producer
```powershell
.\producer_start.bat raw_wiki_changes
```
Usage: `producer_start.bat <topic_name>`
Connects to Wikimedia EventStreams and forwards Wikipedia edit events to Kafka.

### Step 4 — Start consumer
```powershell
.\consumer_start.bat raw_wiki_changes
```
Usage: `consumer_start.bat <topic_name>`
Reads from Kafka and writes to Postgres `wiki_changes_bronze` table every 10 seconds.

### Step 5 — Shutdown (always use this, never close windows manually)
```powershell
.\kill_switch.bat
```
Stops Kafka first, then Zookeeper — ensures clean shutdown with no stale Zookeeper nodes.

## Bat file responsibilities

| File | Purpose |
|---|---|
| `infra_start.bat` | Start Zookeeper + Kafka only |
| `create_topic.bat` | Create a Kafka topic on demand |
| `producer_start.bat` | Start producer for a given topic |
| `consumer_start.bat` | Start Spark consumer for a given topic |
| `kill_switch.bat` | Stop all services cleanly |

## Verifying the pipeline

Check messages in Kafka topic:
```powershell
C:\kafka\bin\windows\kafka-console-consumer.bat --topic raw_wiki_changes --bootstrap-server 127.0.0.1:9092 --from-beginning
```

Check rows in Postgres:
```sql
SELECT COUNT(*) FROM wiki_changes_bronze;
SELECT * FROM wiki_changes_bronze ORDER BY ingested_at DESC LIMIT 10;
```

## Roadmap

- [x] Producer — Wikimedia SSE → Kafka
- [x] Consumer — Kafka → Postgres (Bronze)
- [ ] Validate end-to-end locally
- [ ] Silver/Gold transformations
- [ ] Docker packaging
- [ ] Orchestration (Airflow) — later