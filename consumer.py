"""
Kafka (raw_wiki_changes) -> Postgres (wiki_changes_bronze) via PySpark Structured Streaming.

Reads the JSON events the producer pushed to Kafka, parses them against a fixed
schema (must match producer.normalize()), and micro-batches them into Postgres
using foreachBatch + JDBC.

Run locally (no Docker) with, e.g.:

    spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
        consumer.py
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, from_unixtime, to_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType,
    LongType, DoubleType,
)

import time
from dotenv import load_dotenv
load_dotenv()



INSTANCE_ID = str(int(time.time()))
CHECKPOINT_LOCATION = f"./_checkpoints/wiki_changes_bronze_{INSTANCE_ID}"


# --- Config (env vars, with sensible 127.0.0.1 defaults) ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "no_topic_specified")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "127.0.0.1")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_TABLE = os.getenv("POSTGRES_TABLE")

JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
JDBC_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
}

# Must mirror the dict shape produced by producer.normalize()
EVENT_SCHEMA = StructType([
    StructField("wiki", StringType()),
    StructField("type", StringType()),
    StructField("title", StringType()),
    StructField("user", StringType()),
    StructField("bot", BooleanType()),
    StructField("minor", BooleanType()),
    StructField("comment", StringType()),
    StructField("timestamp", DoubleType()),      # epoch seconds
    StructField("server_name", StringType()),
    StructField("length_old", LongType()),
    StructField("length_new", LongType()),
    StructField("rev_old", LongType()),
    StructField("rev_new", LongType()),
    StructField("meta_id", StringType()),
    StructField("meta_dt", StringType()),         # ISO 8601 string
])


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("wiki-changes-kafka-to-postgres")
        .getOrCreate()
    )


def write_batch_to_postgres(batch_df, batch_id: int):
    """foreachBatch sink: dedupe within the micro-batch, then append to Postgres."""
    if batch_df.count() == 0:
        return

    deduped = batch_df.dropDuplicates(["meta_id"])

    (
        deduped.write
        .mode("append")
        .jdbc(url=JDBC_URL, table=POSTGRES_TABLE, properties=JDBC_PROPERTIES)
    )

    print(f"[batch {batch_id}] wrote {deduped.count()} rows to {POSTGRES_TABLE}")


def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), EVENT_SCHEMA).alias("data"))
        .select("data.*")
        .withColumnRenamed("type", "change_type")
        .withColumnRenamed("bot", "is_bot")
        .withColumnRenamed("minor", "is_minor")
        .withColumnRenamed("user", "user")
        .withColumn("event_timestamp", to_timestamp(from_unixtime(col("timestamp"))))
        .withColumn("meta_dt", to_timestamp(col("meta_dt")))
        .drop("timestamp")
    )

    query = (
        parsed.writeStream
        .foreachBatch(write_batch_to_postgres)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(processingTime="10 seconds")
        .start()
    )

    print(
        f"Streaming {KAFKA_TOPIC} -> {POSTGRES_TABLE} "
        f"(kafka={KAFKA_BOOTSTRAP_SERVERS}, pg={JDBC_URL})"
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()