#!/usr/bin/env python3
"""
NYC TLC Yellow Taxi HDFS Batch + Reference Loader.

Batch path:
    Historical monthly Yellow Taxi Parquet files -> HDFS Bronze /batch

Streaming month:
    Exactly one month is excluded from batch ingestion because it follows:
        Parquet -> Kafka Producer -> Kafka -> Spark Structured Streaming
        -> HDFS Bronze /streaming

Static reference path (optional):
    Taxi-zone shapefile components and Taxi Zone Lookup CSV
        -> HDFS /reference

The reference files are static. They are loaded once and are NEVER sent
through Kafka.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from collections import defaultdict
from pathlib import Path
from urllib.parse import urlparse

import pyarrow.parquet as pq
from hdfs import InsecureClient
from hdfs.util import HdfsError


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

HDFS_WEB_URL = os.environ.get("HDFS_WEB_URL", "http://namenode:9870")
HDFS_USER = os.environ.get("HDFS_USER", "root")

# Example: hdfs://namenode:9000/nyc-taxi/bronze/batch
_BATCH_BRONZE_RAW = os.environ.get(
    "BATCH_BRONZE_PATH",
    "hdfs://namenode:9000/nyc-taxi/bronze/batch",
)
BATCH_BRONZE_PATH = (
    urlparse(_BATCH_BRONZE_RAW).path or "/nyc-taxi/bronze/batch"
)

# Example: hdfs://namenode:9000/nyc-taxi/reference
_REFERENCE_RAW = os.environ.get(
    "REFERENCE_HDFS_PATH",
    "hdfs://namenode:9000/nyc-taxi/reference",
)
REFERENCE_HDFS_PATH = (
    urlparse(_REFERENCE_RAW).path or "/nyc-taxi/reference"
)
TAXI_ZONE_HDFS_DIR = f"{REFERENCE_HDFS_PATH}/taxi_zones"

BATCH_CONTROL_ROOT = "/nyc-taxi/control/batch_loaded"
REFERENCE_CONTROL_ROOT = "/nyc-taxi/control/reference_loaded"
HDFS_RETRIES = 4

# TLC monthly files are normally named like:
# yellow_tripdata_2024-01.parquet
YEAR_MONTH_RE = re.compile(r"(20\d{2})-(0[1-9]|1[0-2])")

REQUIRED_SHAPEFILE_EXTENSIONS = (".shp", ".dbf", ".shx", ".prj")
OPTIONAL_SHAPEFILE_EXTENSIONS = (".cpg",)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("hdfs_batch_loader")


# ---------------------------------------------------------------------------
# HDFS helpers
# ---------------------------------------------------------------------------

def connect_hdfs(retries: int = 10, backoff: float = 3.0) -> InsecureClient:
    """Connect to HDFS through WebHDFS."""
    attempt = 0

    while True:
        attempt += 1
        try:
            client = InsecureClient(HDFS_WEB_URL, user=HDFS_USER)
            client.status("/")
            log.info(
                "Connected to HDFS (WebHDFS) at %s as user=%s",
                HDFS_WEB_URL,
                HDFS_USER,
            )
            return client

        except Exception as exc:
            if attempt >= retries:
                log.error(
                    "Could not reach HDFS at %s after %d attempts.",
                    HDFS_WEB_URL,
                    attempt,
                )
                raise

            log.warning(
                "HDFS not reachable at %s (attempt %d/%d): %s "
                "- retrying in %.1fs",
                HDFS_WEB_URL,
                attempt,
                retries,
                exc,
                backoff,
            )
            time.sleep(backoff)


def write_file_to_hdfs(
    client: InsecureClient,
    local_file: Path,
    hdfs_path: str,
) -> None:
    """Upload one local file to HDFS with retries."""
    last_exc = None

    for attempt in range(1, HDFS_RETRIES + 1):
        try:
            with local_file.open("rb") as source:
                with client.write(hdfs_path, overwrite=True) as writer:
                    while True:
                        chunk = source.read(8 * 1024 * 1024)  # 8 MB
                        if not chunk:
                            break
                        writer.write(chunk)
            return

        except (HdfsError, OSError) as exc:
            last_exc = exc
            log.warning(
                "HDFS write attempt %d/%d failed for %s: %s",
                attempt,
                HDFS_RETRIES,
                hdfs_path,
                exc,
            )
            time.sleep(min(2 ** attempt, 15))

    raise RuntimeError(
        f"Giving up writing {hdfs_path} after {HDFS_RETRIES} attempts"
    ) from last_exc


# ---------------------------------------------------------------------------
# Batch-control helpers
# ---------------------------------------------------------------------------

def month_marker_dir(year_month: str) -> str:
    return f"{BATCH_CONTROL_ROOT}/{year_month}"


def is_month_already_loaded(
    client: InsecureClient,
    year_month: str,
) -> bool:
    """Return True when this batch month already has a _SUCCESS marker."""
    marker = f"{month_marker_dir(year_month)}/_SUCCESS"
    return client.status(marker, strict=False) is not None


def mark_month_loaded(
    client: InsecureClient,
    year_month: str,
    row_count: int,
    file_count: int,
) -> None:
    """Create a success marker after every file for a month is loaded."""
    marker_dir = month_marker_dir(year_month)
    marker = f"{marker_dir}/_SUCCESS"

    payload = json.dumps(
        {
            "year_month": year_month,
            "row_count": row_count,
            "source_files": file_count,
        },
        indent=2,
    )

    client.makedirs(marker_dir)
    with client.write(marker, overwrite=True, encoding="utf-8") as writer:
        writer.write(payload)


# ---------------------------------------------------------------------------
# Local trip-data helpers
# ---------------------------------------------------------------------------

def extract_year_month(file_path: Path) -> str | None:
    """Extract YYYY-MM from a TLC monthly Parquet filename."""
    match = YEAR_MONTH_RE.search(file_path.name)
    if not match:
        return None

    year, month = match.groups()
    return f"{year}-{month}"


def parquet_row_count(file_path: Path) -> int:
    """Read only Parquet metadata to get the number of rows."""
    parquet_file = pq.ParquetFile(file_path)
    return parquet_file.metadata.num_rows


def discover_batch_files(
    source_dir: Path,
    streaming_month: str,
) -> dict[str, list[Path]]:
    """Discover Parquet files and exclude the one streaming month."""
    grouped: dict[str, list[Path]] = defaultdict(list)

    for file_path in sorted(source_dir.rglob("*.parquet")):
        year_month = extract_year_month(file_path)

        if year_month is None:
            log.warning(
                "Skipping file because YYYY-MM could not be found in its "
                "name: %s",
                file_path,
            )
            continue

        if year_month == streaming_month:
            log.info(
                "[%s] STREAMING MONTH -> excluded from batch ingestion: %s",
                year_month,
                file_path,
            )
            continue

        grouped[year_month].append(file_path)

    return grouped


# ---------------------------------------------------------------------------
# Static reference-data helpers
# ---------------------------------------------------------------------------

def _find_taxi_zone_shapefile(reference_dir: Path) -> Path | None:
    """Find the taxi-zone .shp file, including names such as taxi_zones(1).shp."""
    candidates = [
        path
        for path in reference_dir.rglob("*.shp")
        if "taxi_zone" in path.stem.lower()
    ]

    if not candidates:
        return None

    return sorted(candidates)[0]


def discover_taxi_zone_files(reference_dir: Path) -> list[Path]:
    """
    Discover one complete taxi-zone shapefile set.

    Required:
        .shp, .dbf, .shx, .prj
    Optional:
        .cpg
    """
    shp_file = _find_taxi_zone_shapefile(reference_dir)
    if shp_file is None:
        raise FileNotFoundError(
            f"No taxi-zone .shp file found under {reference_dir}"
        )

    base = shp_file.with_suffix("")
    files: list[Path] = []

    for extension in REQUIRED_SHAPEFILE_EXTENSIONS:
        component = base.with_suffix(extension)
        if not component.exists():
            raise FileNotFoundError(
                f"Taxi-zone shapefile is incomplete. Missing: {component}"
            )
        files.append(component)

    for extension in OPTIONAL_SHAPEFILE_EXTENSIONS:
        component = base.with_suffix(extension)
        if component.exists():
            files.append(component)

    return files


def discover_taxi_zone_lookup(reference_dir: Path) -> Path | None:
    """Find an optional Taxi Zone Lookup CSV if it is present."""
    candidates = []

    for path in reference_dir.rglob("*.csv"):
        normalized = path.stem.lower().replace("-", "_").replace(" ", "_")
        if "taxi_zone_lookup" in normalized:
            candidates.append(path)

    if not candidates:
        return None

    return sorted(candidates)[0]


def reference_success_marker() -> str:
    return f"{REFERENCE_CONTROL_ROOT}/_SUCCESS"


def is_reference_already_loaded(client: InsecureClient) -> bool:
    return client.status(reference_success_marker(), strict=False) is not None


def mark_reference_loaded(
    client: InsecureClient,
    source_files: list[str],
) -> None:
    client.makedirs(REFERENCE_CONTROL_ROOT)

    payload = json.dumps(
        {
            "reference_root": REFERENCE_HDFS_PATH,
            "source_files": source_files,
        },
        indent=2,
    )

    with client.write(
        reference_success_marker(),
        overwrite=True,
        encoding="utf-8",
    ) as writer:
        writer.write(payload)


def load_reference_data(
    client: InsecureClient,
    reference_dir: Path,
    force_reload: bool,
) -> list[str]:
    """
    Load taxi-zone reference files once into HDFS.

    Destination:
        /nyc-taxi/reference/taxi_zones/taxi_zones.shp
        /nyc-taxi/reference/taxi_zones/taxi_zones.dbf
        /nyc-taxi/reference/taxi_zones/taxi_zones.shx
        /nyc-taxi/reference/taxi_zones/taxi_zones.prj
        /nyc-taxi/reference/taxi_zones/taxi_zones.cpg  (if present)

    If a Taxi Zone Lookup CSV is present, it is loaded as:
        /nyc-taxi/reference/taxi_zone_lookup.csv
    """
    if is_reference_already_loaded(client) and not force_reload:
        log.info(
            "Reference data already loaded (_SUCCESS found) - skipping."
        )
        return []

    shape_files = discover_taxi_zone_files(reference_dir)
    lookup_csv = discover_taxi_zone_lookup(reference_dir)

    client.makedirs(TAXI_ZONE_HDFS_DIR)
    client.makedirs(REFERENCE_HDFS_PATH)

    loaded: list[str] = []

    for local_file in shape_files:
        canonical_name = f"taxi_zones{local_file.suffix.lower()}"
        hdfs_path = f"{TAXI_ZONE_HDFS_DIR}/{canonical_name}"

        log.info(
            "REFERENCE uploading %s -> %s",
            local_file,
            hdfs_path,
        )
        write_file_to_hdfs(client, local_file, hdfs_path)
        loaded.append(str(local_file))

    if lookup_csv is not None:
        hdfs_path = f"{REFERENCE_HDFS_PATH}/taxi_zone_lookup.csv"
        log.info(
            "REFERENCE uploading %s -> %s",
            lookup_csv,
            hdfs_path,
        )
        write_file_to_hdfs(client, lookup_csv, hdfs_path)
        loaded.append(str(lookup_csv))
    else:
        log.warning(
            "No Taxi Zone Lookup CSV found in %s. "
            "The shapefile set will still be loaded.",
            reference_dir,
        )

    mark_reference_loaded(client, loaded)
    log.info("Reference data load COMPLETE (%d file(s)).", len(loaded))
    return loaded


# ---------------------------------------------------------------------------
# Batch loading
# ---------------------------------------------------------------------------

def load_month(
    client: InsecureClient,
    year_month: str,
    files: list[Path],
) -> tuple[int, int]:
    """Upload one batch month to /bronze/batch/year=YYYY/month=MM/."""
    year, month = year_month.split("-")
    hdfs_dir = f"{BATCH_BRONZE_PATH}/year={year}/month={month}"

    client.makedirs(hdfs_dir)
    total_rows = 0

    for seq, local_file in enumerate(files, start=1):
        destination_name = f"part-{seq:04d}-{local_file.name}"
        hdfs_path = f"{hdfs_dir}/{destination_name}"
        rows = parquet_row_count(local_file)

        log.info(
            "[%s] uploading %s (%d rows) -> %s",
            year_month,
            local_file,
            rows,
            hdfs_path,
        )

        write_file_to_hdfs(client, local_file, hdfs_path)
        total_rows += rows

    return total_rows, len(files)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Yellow Taxi ingestion loader: historical Parquet -> HDFS Bronze, "
            "excluding one streaming month, plus optional static taxi-zone "
            "reference data."
        )
    )

    parser.add_argument(
        "--source-dir",
        required=True,
        help="Directory containing historical Yellow Taxi Parquet files.",
    )

    parser.add_argument(
        "--streaming-month",
        required=True,
        help=(
            "Exactly one YYYY-MM month reserved for Kafka/Spark Structured "
            "Streaming, for example: 2025-01"
        ),
    )

    parser.add_argument(
        "--reference-dir",
        default=None,
        help=(
            "Optional directory containing taxi_zones.shp/.dbf/.shx/.prj/"
            ".cpg and optionally taxi_zone_lookup.csv. These files are loaded "
            "once into HDFS /nyc-taxi/reference and are never sent to Kafka."
        ),
    )

    parser.add_argument(
        "--reload-reference",
        action="store_true",
        help="Force re-upload of static reference files even if _SUCCESS exists.",
    )

    args = parser.parse_args()

    source_dir = Path(args.source_dir).resolve()
    streaming_month = args.streaming_month
    reference_dir = (
        Path(args.reference_dir).resolve()
        if args.reference_dir is not None
        else None
    )

    if not YEAR_MONTH_RE.fullmatch(streaming_month):
        log.error("--streaming-month must be in YYYY-MM format, e.g. 2025-01")
        return 2

    if not source_dir.exists() or not source_dir.is_dir():
        log.error("Source directory does not exist: %s", source_dir)
        return 2

    if reference_dir is not None and (
        not reference_dir.exists() or not reference_dir.is_dir()
    ):
        log.error("Reference directory does not exist: %s", reference_dir)
        return 2

    log.info("=" * 70)
    log.info("NYC YELLOW TAXI - BATCH + REFERENCE INGESTION")
    log.info("Source directory : %s", source_dir)
    log.info("Streaming month  : %s (WILL BE SKIPPED)", streaming_month)
    log.info("HDFS batch path  : %s", BATCH_BRONZE_PATH)
    if reference_dir is not None:
        log.info("Reference dir    : %s", reference_dir)
        log.info("HDFS reference   : %s", REFERENCE_HDFS_PATH)
    else:
        log.info("Reference data   : not requested")
    log.info("=" * 70)

    batch_files = discover_batch_files(
        source_dir=source_dir,
        streaming_month=streaming_month,
    )

    if not batch_files and reference_dir is None:
        log.warning("No batch Parquet files were found.")
        return 0

    client = connect_hdfs()

    reference_failed = False
    reference_loaded_count = 0

    if reference_dir is not None:
        try:
            loaded_reference_files = load_reference_data(
                client=client,
                reference_dir=reference_dir,
                force_reload=args.reload_reference,
            )
            reference_loaded_count = len(loaded_reference_files)
        except Exception as exc:
            log.exception("REFERENCE load FAILED: %s", exc)
            reference_failed = True

    loaded_months: set[str] = set()
    skipped_months: set[str] = set()
    failed_months: set[str] = set()

    for year_month in sorted(batch_files):
        files = batch_files[year_month]

        try:
            if is_month_already_loaded(client, year_month):
                log.info(
                    "[%s] already loaded (_SUCCESS found) - skipping.",
                    year_month,
                )
                skipped_months.add(year_month)
                continue

            total_rows, file_count = load_month(
                client=client,
                year_month=year_month,
                files=files,
            )

            mark_month_loaded(
                client=client,
                year_month=year_month,
                row_count=total_rows,
                file_count=file_count,
            )

            loaded_months.add(year_month)
            log.info(
                "[%s] COMPLETE (%d rows, %d source file(s)).",
                year_month,
                total_rows,
                file_count,
            )

        except Exception as exc:
            log.exception("[%s] FAILED: %s", year_month, exc)
            failed_months.add(year_month)

    log.info("=" * 70)
    log.info("SUMMARY")
    log.info("  Streaming month excluded : %s", streaming_month)
    log.info("  Batch loaded this run    : %d month(s)", len(loaded_months))
    log.info("  Batch already loaded     : %d month(s)", len(skipped_months))
    log.info(
        "  Batch failed             : %d month(s) -> %s",
        len(failed_months),
        sorted(failed_months),
    )
    if reference_dir is not None:
        log.info("  Reference files uploaded : %d", reference_loaded_count)
        log.info(
            "  Reference status         : %s",
            "FAILED" if reference_failed else "OK",
        )
    log.info("=" * 70)

    return 0 if not failed_months and not reference_failed else 1


if __name__ == "__main__":
    sys.exit(main())
