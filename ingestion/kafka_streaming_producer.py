#!/usr/bin/env python3
"""
NYC TLC Yellow Taxi Kafka Streaming Producer.

Streaming path:
    ONE monthly Yellow Taxi Parquet file
        -> Kafka Producer
        -> Kafka topic
        -> Spark Structured Streaming

This script is ONLY for the selected streaming month.
Historical/batch ingestion should be handled by a separate batch loader.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable


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

YEAR_MONTH_RE = re.compile(r"(20\d{2})-(0[1-9]|1[0-2])")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

log = logging.getLogger("yellow_taxi_kafka_producer")


# ---------------------------------------------------------------------------
# JSON serialization helpers
# ---------------------------------------------------------------------------

def json_safe(value):
    """
    Convert values commonly found in Parquet/PyArrow data into JSON-safe values.
    """
    if isinstance(value, (datetime, date)):
        return value.isoformat()

    if isinstance(value, Decimal):
        return float(value)

    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")

    return value


def serialize_record(record: dict) -> bytes:
    """
    Convert one taxi row into UTF-8 JSON bytes.
    """
    clean_record = {
        key: json_safe(value)
        for key, value in record.items()
    }

    return json.dumps(
        clean_record,
        ensure_ascii=False,
        default=str,
    ).encode("utf-8")


# ---------------------------------------------------------------------------
# File / month helpers
# ---------------------------------------------------------------------------

def extract_year_month(file_path: Path) -> str | None:
    """
    Extract YYYY-MM from a TLC filename.

    Example:
        yellow_tripdata_2025-01.parquet
        -> 2025-01
    """
    match = YEAR_MONTH_RE.search(file_path.name)

    if not match:
        return None

    year, month = match.groups()
    return f"{year}-{month}"


# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------

def connect_producer(
    retries: int = 10,
    backoff: float = 3.0,
) -> KafkaProducer:
    """
    Connect to Kafka with retries.
    """
    attempt = 0

    while True:
        attempt += 1

        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,

                # We serialize manually because taxi rows may contain dates,
                # decimals, and other values that json.dumps cannot serialize
                # directly without conversion.
                key_serializer=lambda key: (
                    key.encode("utf-8")
                    if key is not None
                    else None
                ),

                acks="all",
                retries=5,
                linger_ms=10,
                batch_size=32_768,
            )

            # Force Kafka metadata lookup now so connection problems
            # happen before we start reading millions of taxi rows.
            producer.bootstrap_connected()

            log.info(
                "Connected to Kafka at %s",
                KAFKA_BOOTSTRAP,
            )

            return producer

        except NoBrokersAvailable as exc:
            if attempt >= retries:
                log.error(
                    "Could not connect to Kafka at %s after %d attempts.",
                    KAFKA_BOOTSTRAP,
                    attempt,
                )
                raise

            log.warning(
                "Kafka unavailable at %s (attempt %d/%d): %s "
                "- retrying in %.1fs",
                KAFKA_BOOTSTRAP,
                attempt,
                retries,
                exc,
                backoff,
            )

            time.sleep(backoff)


# ---------------------------------------------------------------------------
# Streaming producer logic
# ---------------------------------------------------------------------------

def send_month(
    producer: KafkaProducer,
    parquet_path: Path,
    year_month: str,
    rows_per_batch: int,
    delay: float,
    max_rows: int | None,
) -> int:
    """
    Read one monthly Parquet file incrementally and send every taxi trip
    as an individual Kafka message.

    The Kafka message key is YYYY-MM.

    At the end, an EOF control message is sent:
        {"__eof__": true, "year_month": "YYYY-MM"}
    """
    parquet_file = pq.ParquetFile(parquet_path)

    total_rows = parquet_file.metadata.num_rows

    log.info("=" * 65)
    log.info("STREAMING MONTH : %s", year_month)
    log.info("SOURCE FILE     : %s", parquet_path)
    log.info("PARQUET ROWS    : %d", total_rows)
    log.info("KAFKA TOPIC     : %s", KAFKA_STREAM_TOPIC)
    log.info("KAFKA BROKER    : %s", KAFKA_BOOTSTRAP)

    if delay > 0:
        log.info(
            "SIMULATION DELAY: %.4f second(s) between records",
            delay,
        )
    else:
        log.info("SIMULATION DELAY: disabled")

    if max_rows is not None:
        log.info("MAX ROWS        : %d", max_rows)

    log.info("=" * 65)

    sent = 0

    try:
        # iter_batches prevents loading the entire month into RAM.
        for record_batch in parquet_file.iter_batches(
            batch_size=rows_per_batch
        ):
            rows = record_batch.to_pylist()

            for row in rows:
                if max_rows is not None and sent >= max_rows:
                    break

                future = producer.send(
                    topic=KAFKA_STREAM_TOPIC,
                    key=year_month,
                    value=serialize_record(row),
                )

                # Don't block on every send. KafkaProducer sends asynchronously.
                # Errors are surfaced when flush() runs.
                sent += 1

                if sent % 10_000 == 0:
                    producer.flush()
                    log.info(
                        "[%s] sent %s taxi records",
                         year_month,
                        f"{sent:,}",
                    )

                if delay > 0:
                    time.sleep(delay)

            if max_rows is not None and sent >= max_rows:
                break

        # Ensure all taxi rows are acknowledged before sending EOF.
        producer.flush()

        eof_message = {
            "__eof__": True,
            "year_month": year_month,
            "rows_sent": sent,
        }

        producer.send(
            topic=KAFKA_STREAM_TOPIC,
            key=year_month,
            value=json.dumps(eof_message).encode("utf-8"),
        ).get(timeout=30)

        producer.flush()

        log.info(
            "[%s] EOF sent. Streaming producer COMPLETE.",
            year_month,
        )

        log.info(
            "[%s] sent %s taxi records",
            year_month,
            f"{sent:,}",
)

        return sent

    except KafkaError:
        log.exception(
            "[%s] Kafka error while sending records.",
            year_month,
        )
        raise


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Simulate streaming for exactly one NYC Yellow Taxi monthly "
            "Parquet file by publishing its rows to Kafka."
        )
    )

    parser.add_argument(
        "--file",
        required=True,
        help=(
            "Path to exactly ONE monthly Yellow Taxi Parquet file, "
            "for example yellow_tripdata_2025-01.parquet"
        ),
    )

    parser.add_argument(
        "--month",
        default=None,
        help=(
            "Optional YYYY-MM streaming month. If omitted, it is extracted "
            "from the filename."
        ),
    )

    parser.add_argument(
        "--delay",
        type=float,
        default=0.01,
        help=(
            "Delay in seconds between records to simulate live streaming. "
            "Default: 0.01. Use 0 for maximum speed."
        ),
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=10_000,
        help=(
            "Number of Parquet rows read into memory at once. "
            "Default: 10000."
        ),
    )

    parser.add_argument(
        "--max-rows",
        type=int,
        default=None,
        help=(
            "Optional limit for testing. Example: --max-rows 10000"
        ),
    )

    args = parser.parse_args()

    parquet_path = Path(args.file).resolve()

    if not parquet_path.exists():
        log.error(
            "Parquet file does not exist: %s",
            parquet_path,
        )
        return 2

    if not parquet_path.is_file():
        log.error(
            "Expected a file, but received: %s",
            parquet_path,
        )
        return 2

    if parquet_path.suffix.lower() != ".parquet":
        log.error(
            "Streaming source must be a .parquet file."
        )
        return 2

    filename_month = extract_year_month(parquet_path)

    year_month = args.month or filename_month

    if year_month is None:
        log.error(
            "Could not determine YYYY-MM from filename. "
            "Provide --month YYYY-MM."
        )
        return 2

    if not YEAR_MONTH_RE.fullmatch(year_month):
        log.error(
            "Month must be in YYYY-MM format, e.g. 2025-01."
        )
        return 2

    if (
        args.month is not None
        and filename_month is not None
        and args.month != filename_month
    ):
        log.error(
            "Provided month %s does not match filename month %s.",
            args.month,
            filename_month,
        )
        return 2

    if args.delay < 0:
        log.error("--delay cannot be negative.")
        return 2

    if args.batch_size <= 0:
        log.error("--batch-size must be greater than zero.")
        return 2

    if args.max_rows is not None and args.max_rows <= 0:
        log.error("--max-rows must be greater than zero.")
        return 2

    producer = None

    try:
        producer = connect_producer()

        send_month(
            producer=producer,
            parquet_path=parquet_path,
            year_month=year_month,
            rows_per_batch=args.batch_size,
            delay=args.delay,
            max_rows=args.max_rows,
        )

        return 0

    except Exception as exc:
        log.error(
            "Streaming producer failed: %s",
            exc,
        )
        return 1

    finally:
        if producer is not None:
            producer.close()


if __name__ == "__main__":
    sys.exit(main())
