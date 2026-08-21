"""
Wikimedia RecentChange -> Kafka producer.

Connects to Wikimedia's public EventStreams SSE endpoint, which is a live
firehose of every edit/create/delete/log event across all public Wikimedia
wikis, and forwards a filtered subset of that stream to a local Kafka topic.

Source: https://stream.wikimedia.org/v2/stream/recentchange
Docs:   https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams_HTTP_Service
"""

import json
import logging
import os, sys
import time

from dotenv import load_dotenv
load_dotenv()

import requests
import sseclient
from confluent_kafka import Producer
from confluent_kafka import KafkaException

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("wiki-producer")

# --- Config (all overridable via environment variables) ---
STREAM_URL = os.getenv("WIKI_STREAM_URL", "https://stream.wikimedia.org/v2/stream/recentchange")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
KAFKA_TOPIC = sys.argv[1] if len(sys.argv) > 1 else os.getenv("KAFKA_TOPIC", "no_topic_specified")

WIKI_FILTER = {w.strip() for w in os.getenv("WIKI_FILTER", "enwiki").split(",") if w.strip()}
TYPE_FILTER = {t.strip() for t in os.getenv("TYPE_FILTER", "edit,new").split(",") if t.strip()}
EXCLUDE_BOTS = os.getenv("EXCLUDE_BOTS", "true").lower() == "true"

RECONNECT_BACKOFF_SECONDS = 5
KAFKA_CONNECT_RETRY_SECONDS = 5


def build_kafka_producer() -> Producer:
    """Create a confluent_kafka Producer, retrying until the broker is reachable."""
    while True:
        try:
            producer = Producer({
                'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
                'retries': 5,
                'linger.ms': 200,
                'acks': 'all',
            })
            # Test connection by fetching metadata
            producer.list_topics(timeout=10)
            log.info("Connected to Kafka at %s", KAFKA_BOOTSTRAP_SERVERS)
            return producer
        except KafkaException as e:
            log.warning("Kafka not ready yet (%s); retrying in %ss", e, KAFKA_CONNECT_RETRY_SECONDS)
            time.sleep(KAFKA_CONNECT_RETRY_SECONDS)


def delivery_report(err, msg):
    """Callback fired when a message is delivered or fails."""
    if err is not None:
        log.error("Message delivery failed: %s", err)


def should_keep(event: dict) -> bool:
    if WIKI_FILTER and event.get("wiki") not in WIKI_FILTER:
        return False
    if TYPE_FILTER and event.get("type") not in TYPE_FILTER:
        return False
    if EXCLUDE_BOTS and event.get("bot"):
        return False
    return True


def normalize(event: dict) -> dict:
    return {
        "wiki": event.get("wiki"),
        "type": event.get("type"),
        "title": event.get("title"),
        "user": event.get("user"),
        "bot": event.get("bot", False),
        "minor": event.get("minor", False),
        "comment": event.get("comment"),
        "timestamp": event.get("timestamp"),
        "server_name": event.get("server_name"),
        "length_old": (event.get("length") or {}).get("old"),
        "length_new": (event.get("length") or {}).get("new"),
        "rev_old": (event.get("revision") or {}).get("old"),
        "rev_new": (event.get("revision") or {}).get("new"),
        "meta_id": (event.get("meta") or {}).get("id"),
        "meta_dt": (event.get("meta") or {}).get("dt"),
    }


def run():
    producer = build_kafka_producer()
    log.info(
        "Starting stream consumer | wikis=%s types=%s exclude_bots=%s topic=%s",
        WIKI_FILTER or "ALL", TYPE_FILTER or "ALL", EXCLUDE_BOTS, KAFKA_TOPIC,
    )

    sent, skipped = 0, 0
    last_log = time.time()

    while True:
        try:
            resp = requests.get(
                STREAM_URL,
                stream=True,
                headers={
                    "Accept": "text/event-stream",
                    "User-Agent": "wiki-changeset-streaming/1.0 (https://github.com/ganeshmehta/alphaTrader; ganesh@example.com) python-requests"
                }
            )
            resp.raise_for_status()
            client = sseclient.SSEClient(resp)
            log.info("SSE stream connected. Listening for events...")

            for sse_event in client.events():
                if not sse_event.data:
                    continue
                try:
                    event = json.loads(sse_event.data)
                except json.JSONDecodeError:
                    continue

                if not should_keep(event):
                    skipped += 1
                    continue

                payload = normalize(event)
                key = payload["wiki"] or "unknown"

                producer.produce(
                    KAFKA_TOPIC,
                    key=key.encode("utf-8"),
                    value=json.dumps(payload).encode("utf-8"),
                    callback=delivery_report
                )
                producer.poll(0)
                sent += 1

                if time.time() - last_log > 30:
                    log.info("sent=%d skipped=%d (last 30s window)", sent, skipped)
                    sent, skipped = 0, 0
                    last_log = time.time()

        except (requests.RequestException, ConnectionError) as e:
            log.warning("Stream connection dropped (%s); reconnecting in %ss", e, RECONNECT_BACKOFF_SECONDS)
            time.sleep(RECONNECT_BACKOFF_SECONDS)
        except KeyboardInterrupt:
            log.info("Shutting down.")
            producer.flush()
            break


if __name__ == "__main__":
    run()