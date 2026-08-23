# Wiki Changeset Streaming — Real-Time Wikipedia Analytics Pipeline

A production-grade real-time data pipeline that ingests live Wikipedia edit events, processes them through Apache Kafka and PySpark Structured Streaming, stores in PostgreSQL, caches real-time metrics in Redis, and visualizes on a Grafana dashboard.

## Architecture

```
Wikimedia EventStreams (SSE)
        ↓
producer.py (confluent-kafka)
        ↓
Apache Kafka — topic: raw_wiki_changes (3 partitions, MurmurHash2 routing)
    Partition 0 → enwiki
    Partition 1 → commonswiki
    Partition 2 → ruwiki
        ↓
consumer.py (PySpark Structured Streaming)
        ↓
    ┌───────────────────────────────┐
    │  PostgreSQL                   │      Redis
    │  wiki_changes_bronze (Bronze) │  +   edit_velocity:{wiki}   → Sorted Set
    │  Raw event storage            │      top_editors:{wiki}      → Sorted Set
    │  Permanent historical store   │      daily_counts:{wiki}     → Hash
    └───────────────────────────────┘
                    ↓
            Grafana Dashboard
            - Edit velocity (last 10 min)
            - Top editors per wiki
            - Edits per hour (last 24h)
```

## Tech Stack

| Component | Technology | Purpose |
|---|---|---|
| Data Source | Wikimedia EventStreams (SSE) | Live Wikipedia edits |
| Message Broker | Apache Kafka 3.8.0 | Real-time event streaming |
| Stream Processing | PySpark Structured Streaming 3.5.1 | Micro-batch processing |
| Primary Storage | PostgreSQL 16 | Bronze layer — raw event storage |
| Cache | Redis (Memurai) | Real-time metrics for dashboard |
| Visualization | Grafana | Live analytics dashboard |
| Language | Python 3.14 | Producer, consumer, metrics writer |
| Kafka Client | confluent-kafka | Producer (Python 3.14 compatible) |

## Key Design Decisions

- **MurmurHash2 partitioning** — Wiki name used as Kafka message key, automatically routed to consistent partition per wiki
- **3 Kafka partitions** — One per wiki (enwiki, commonswiki, ruwiki), enabling parallel Spark task processing
- **Bronze layer pattern** — Raw events stored as-is in Postgres for full historical replay capability
- **Redis as speed layer** — Pre-aggregated metrics updated on every batch for sub-millisecond Grafana reads
- **confluent-kafka over kafka-python** — Python 3.14 compatibility (kafka-python has async networking issues on 3.14)
- **Idempotent writes** — Unique index on `meta_id` prevents duplicate rows from reprocessing

## Prerequisites

- Java JDK 17 (Eclipse Temurin)
- Apache Kafka 3.8.0 (installed at `C:\kafka`)
- PostgreSQL 16
- Redis / Memurai (Windows)
- Grafana
- Python 3.14 with virtualenv
- Hadoop winutils (at `C:\hadoop\bin`) — required for PySpark on Windows

## Project Structure

```
wiki_changeset_streaming/
├── producer.py              ← Wikimedia SSE → Kafka producer
├── consumer.py              ← Kafka → Postgres + Redis consumer
├── redis_writer.py          ← Redis metrics update module
├── .env                     ← Environment config (not committed)
├── postgres_init/
│   └── 01_bronze_schema.sql ← Bronze table schema
├── infra_start.bat          ← Start Zookeeper + Kafka
├── producer_start.bat       ← Start producer (usage: producer_start.bat <topic>)
├── consumer_start.bat       ← Start consumer (usage: consumer_start.bat <topic>)
├── create_topic.bat         ← Create Kafka topic (usage: create_topic.bat <topic> <partitions> <replication>)
└── kill_switch.bat          ← Stop all services cleanly
```

## Setup

### 1. Create Bronze table in PostgreSQL

Run in pgAdmin Query Tool against your database:
```sql
-- postgres_init/01_bronze_schema.sql
```

### 2. Configure `.env`

```
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092
WIKI_STREAM_URL=https://stream.wikimedia.org/v2/stream/recentchange
WIKI_FILTER=enwiki,commonswiki,ruwiki
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

### 3. Install dependencies

```powershell
pip install -r requirements.txt
```

## Running Locally (Windows)

### Step 1 — Start infrastructure
```powershell
.\infra_start.bat
```
Starts Zookeeper and Kafka broker. Run once — keep running.

### Step 2 — Create Kafka topic
```powershell
.\create_topic.bat raw_wiki_changes 3 1
```

### Step 3 — Start producer
```powershell
.\producer_start.bat raw_wiki_changes
```
Connects to Wikimedia EventStreams, filters to enwiki/commonswiki/ruwiki human edits, forwards to Kafka.

### Step 4 — Start consumer
```powershell
.\consumer_start.bat raw_wiki_changes
```
PySpark Structured Streaming job — reads Kafka, writes to Postgres + Redis every 10 seconds.

### Step 5 — Backfill Redis from Postgres (optional)
```powershell
python redis_writer.py
```
Populates Redis with last 24 hours of historical data from Postgres on startup.

### Step 6 — Shutdown (always use this)
```powershell
.\kill_switch.bat
```
Stops Kafka first, then Zookeeper — clean shutdown prevents NodeExistsException on restart.

## Verifying the Pipeline

**Check Kafka messages:**
```powershell
C:\kafka\bin\windows\kafka-console-consumer.bat --topic raw_wiki_changes --bootstrap-server 127.0.0.1:9092 --from-beginning --property print.key=true
```

**Check Postgres:**
```sql
SELECT wiki, COUNT(*) FROM wiki_changes_bronze GROUP BY wiki;
```

**Check Redis:**
```powershell
memurai-cli keys *
memurai-cli zrevrange top_editors:enwiki 0 9 withscores
memurai-cli hgetall daily_counts:enwiki
```

**Spark UI:**
```
http://localhost:4040
```

**Grafana Dashboard:**
```
http://localhost:3000
```

## Redis Metrics Schema

| Key Pattern | Type | Contents |
|---|---|---|
| `edit_velocity:{wiki}` | Sorted Set | meta_id → unix timestamp (last 10 min) |
| `top_editors:{wiki}` | Sorted Set | username → edit count (last 10 min) |
| `daily_counts:{wiki}` | Hash | YYYY-MM-DD-HH → edit count (last 24h) |

## Grafana Dashboard Panels

1. **Edit Velocity** — `ZCOUNT edit_velocity:enwiki -inf +inf` → Stat panel
2. **Top Editors** — `ZREVRANGE top_editors:enwiki 0 9 WITHSCORES` → Bar chart
3. **Daily Counts** — PostgreSQL query → Time series chart

## Roadmap

- [x] Producer — Wikimedia SSE → Kafka (3 partitions)
- [x] Consumer — Kafka → PostgreSQL Bronze layer
- [x] Redis — real-time metrics (velocity, top editors, daily counts)
- [x] Grafana — live dashboard
- [ ] Silver/Gold transformations (dbt)
- [ ] Docker packaging
- [ ] Orchestration (Airflow)