#!/usr/bin/env python3
"""
NYC TLC Yellow Taxi Spark Structured Streaming Consumer.

Streaming ingestion path:
    ONE monthly Yellow Taxi Parquet file
        -> Kafka Producer
        -> Kafka topic
        -> Spark Structured Streaming
        -> HDFS Bronze /streaming

This script performs INGESTION/LOADING only.

It intentionally keeps the taxi payload raw in Bronze.
Cleaning, type casting, null handling, joins, and business transformations
belong in the later Bronze -> Silver processing stage.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP = os.environ.get(
    "KAFKA_BOOTSTRAP",
    os.environ.get("KAFKA_BOOTSTRAP_HOST", "kafka:9092"),
)

KAFKA_STREAM_TOPIC = os.environ.get(
    "KAFKA_STREAM_TOPIC",
    "nyc_taxi_stream",
)

STREAM_BRONZE_PATH = os.environ.get(
    "STREAM_BRONZE_PATH",
    "hdfs://namenode:9000/nyc-taxi/bronze/streaming",
)

STREAM_CHECKPOINT_PATH = os.environ.get(
    "STREAM_CHECKPOINT_PATH",
    "hdfs://namenode:9000/nyc-taxi/checkpoints/streaming_ingestion",
)

YEAR_MONTH_RE = re.compile(r"20\d{2}-(0[1-9]|1[0-2])")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

log = logging.getLogger("yellow_taxi_spark_streaming")


# ---------------------------------------------------------------------------
# Spark
# ---------------------------------------------------------------------------

def create_spark_session() -> SparkSession:
    """
    Create the SparkSession used by Structured Streaming.

    The Kafka connector itself must be available to Spark at submit time.
    """
    spark = (
        SparkSession.builder
        .appName("NYC-Yellow-Taxi-Streaming-Ingestion")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(
        os.environ.get("SPARK_LOG_LEVEL", "WARN")
    )

    return spark


# ---------------------------------------------------------------------------
# Kafka source
# ---------------------------------------------------------------------------

def read_kafka_stream(
    spark: SparkSession,
    max_offsets_per_trigger: int | None,
):
    """
    Subscribe to the streaming Kafka topic.

    startingOffsets='earliest' is important for this project because the
    producer may begin publishing before the Spark consumer is started.

    Once a checkpoint exists, Spark resumes from checkpointed offsets.
    """
    reader = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_STREAM_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
    )

    if max_offsets_per_trigger is not None:
        reader = reader.option(
            "maxOffsetsPerTrigger",
            str(max_offsets_per_trigger),
        )

    return reader.load()


# ---------------------------------------------------------------------------
# Bronze transformation
# ---------------------------------------------------------------------------

def prepare_bronze_stream(
    kafka_df,
    streaming_month: str,
):
    """
    Prepare Kafka events for the Bronze HDFS layer.

    Kafka provides key/value as binary. The producer uses:
        key   = YYYY-MM
        value = taxi record JSON

    We:
      1. cast key/value to strings,
      2. verify that only the selected month is accepted,
      3. remove the producer's __eof__ control record,
      4. derive year/month partition columns,
      5. preserve the raw taxi JSON and Kafka metadata.

    No taxi cleaning or business transformation is performed here.
    """

    decoded = kafka_df.select(
        F.col("key").cast("string").alias("year_month"),
        F.col("value").cast("string").alias("raw_json"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    )

    # Detect the producer's special final control message:
    # {"__eof__": true, ...}
    with_control_flag = decoded.withColumn(
        "is_eof",
        F.coalesce(
            F.get_json_object(
                F.col("raw_json"),
                "$.__eof__",
            ).cast("boolean"),
            F.lit(False),
        ),
    )

    # This consumer is deliberately restricted to ONE streaming month.
    taxi_events = with_control_flag.filter(
        (F.col("year_month") == F.lit(streaming_month))
        & (~F.col("is_eof"))
    )

    bronze = (
        taxi_events
        .withColumn(
            "year",
            F.substring(F.col("year_month"), 1, 4),
        )
        .withColumn(
            "month",
            F.substring(F.col("year_month"), 6, 2),
        )
        .select(
            "raw_json",
            "year_month",
            "year",
            "month",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
        )
    )

    return bronze


# ---------------------------------------------------------------------------
# HDFS sink
# ---------------------------------------------------------------------------

def write_to_hdfs(
    bronze_df,
    trigger_seconds: int,
):
    """
    Write the streaming Bronze records to HDFS as Parquet.

    HDFS layout:
        /nyc-taxi/bronze/streaming/
            year=YYYY/
                month=MM/
                    part-....parquet

    Spark's checkpoint stores Kafka progress and streaming state so that a
    restarted query resumes from the correct offsets.
    """

    writer = (
        bronze_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option(
            "checkpointLocation",
            STREAM_CHECKPOINT_PATH,
        )
        .partitionBy(
            "year",
            "month",
        )
    )

    if trigger_seconds > 0:
        writer = writer.trigger(
            processingTime=f"{trigger_seconds} seconds"
        )

    return writer.start(
        STREAM_BRONZE_PATH
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Consume one NYC Yellow Taxi streaming month from Kafka "
            "with Spark Structured Streaming and load it into HDFS Bronze."
        )
    )

    parser.add_argument(
        "--streaming-month",
        required=True,
        help=(
            "The YYYY-MM month being simulated through Kafka. "
            "Example: 2025-01"
        ),
    )

    parser.add_argument(
        "--trigger-seconds",
        type=int,
        default=5,
        help=(
            "Structured Streaming micro-batch interval in seconds. "
            "Default: 5"
        ),
    )

    parser.add_argument(
        "--max-offsets-per-trigger",
        type=int,
        default=None,
        help=(
            "Optional maximum Kafka records processed per Spark trigger. "
            "Useful for testing or controlling throughput."
        ),
    )

    args = parser.parse_args()

    streaming_month = args.streaming_month

    if not YEAR_MONTH_RE.fullmatch(
        streaming_month
    ):
        log.error(
            "--streaming-month must use YYYY-MM format, e.g. 2025-01"
        )
        return 2

    if args.trigger_seconds < 0:
        log.error(
            "--trigger-seconds cannot be negative."
        )
        return 2

    if (
        args.max_offsets_per_trigger is not None
        and args.max_offsets_per_trigger <= 0
    ):
        log.error(
            "--max-offsets-per-trigger must be greater than zero."
        )
        return 2

    spark = None
    query = None

    try:
        log.info("=" * 70)
        log.info("NYC YELLOW TAXI - STREAMING INGESTION")
        log.info(
            "Streaming month : %s",
            streaming_month,
        )
        log.info(
            "Kafka broker    : %s",
            KAFKA_BOOTSTRAP,
        )
        log.info(
            "Kafka topic     : %s",
            KAFKA_STREAM_TOPIC,
        )
        log.info(
            "HDFS Bronze     : %s",
            STREAM_BRONZE_PATH,
        )
        log.info(
            "Checkpoint      : %s",
            STREAM_CHECKPOINT_PATH,
        )
        log.info("=" * 70)

        spark = create_spark_session()

        kafka_df = read_kafka_stream(
            spark=spark,
            max_offsets_per_trigger=args.max_offsets_per_trigger,
        )

        bronze_df = prepare_bronze_stream(
            kafka_df=kafka_df,
            streaming_month=streaming_month,
        )

        log.info(
            "Starting Spark Structured Streaming query..."
        )

        query = write_to_hdfs(
            bronze_df=bronze_df,
            trigger_seconds=args.trigger_seconds,
        )

        log.info(
            "Streaming query started. "
            "Waiting for Kafka records..."
        )

        # The producer's __eof__ record is intentionally filtered out.
        # The query remains alive so it can safely process all records already
        # present in Kafka. Stop it with Ctrl+C after the producer has finished
        # and Spark has processed the final micro-batch.
        query.awaitTermination()

        return 0

    except KeyboardInterrupt:
        log.info(
            "Shutdown requested by user."
        )
        return 0

    except Exception as exc:
        log.exception(
            "Streaming ingestion failed: %s",
            exc,
        )
        return 1

    finally:
        if query is not None:
            try:
                if query.isActive:
                    query.stop()
            except Exception:
                pass

        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    sys.exit(main())
