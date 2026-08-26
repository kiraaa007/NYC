
from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timedelta

import docker
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

STREAMING_MONTH = "2025-12"
BATCH_SOURCE_DIR = "/data/trip_records"
REFERENCE_DIR = "/data/taxi_zones"

SILVER_PATH = "hdfs://namenode:9000/nyc-taxi/silver/trips"
GOLD_PATH = "hdfs://namenode:9000/nyc-taxi/gold"
STREAMING_BRONZE_HDFS = "/nyc-taxi/bronze/streaming/year=2025/month=12"

PIPELINE_STATE_DIR = "/nyc-taxi/control/pipeline_state"
PIPELINE_FINGERPRINT_FILE = f"{PIPELINE_STATE_DIR}/bronze_fingerprint.sha256"

REQUIRED_SERVICES = {
    "namenode",
    "datanode",
    "kafka",
    "spark-master",
    "spark-worker",
    "ingestion",
}


def docker_client():
    try:
        client = docker.from_env()
        client.ping()
        return client
    except Exception as exc:
        raise AirflowException("Airflow cannot reach Docker Engine.") from exc


def find_service_container(service: str):
    client = docker_client()
    containers = client.containers.list(
        filters={
            "label": f"com.docker.compose.service={service}",
            "status": "running",
        }
    )
    if not containers:
        raise AirflowException(
            f"No running Compose container found for service: {service}"
        )
    return client, containers[0]


def exec_in_service(
    service: str,
    command: list[str],
    *,
    expected_text: str | None = None,
) -> str:
    client, container = find_service_container(service)

    exec_info = client.api.exec_create(
        container=container.id,
        cmd=command,
        stdout=True,
        stderr=True,
    )
    exec_id = exec_info["Id"]

    chunks = client.api.exec_start(
        exec_id,
        stream=True,
        demux=True,
    )

    output_parts: list[str] = []

    for stdout_chunk, stderr_chunk in chunks:
        if stdout_chunk:
            text = stdout_chunk.decode("utf-8", errors="replace")
            output_parts.append(text)
            for line in text.rstrip().splitlines():
                log.info("[%s] %s", service, line)

        if stderr_chunk:
            text = stderr_chunk.decode("utf-8", errors="replace")
            output_parts.append(text)
            for line in text.rstrip().splitlines():
                log.warning("[%s] %s", service, line)

    result = client.api.exec_inspect(exec_id)
    exit_code = result.get("ExitCode")
    output = "".join(output_parts)

    if exit_code != 0:
        raise AirflowException(
            f"Command failed in service {service} with exit code {exit_code}."
        )

    if expected_text is not None and expected_text not in output:
        raise AirflowException(
            f"Expected text not found in {service} output: {expected_text!r}"
        )

    return output


def check_required_services():
    client = docker_client()
    running = client.containers.list(filters={"status": "running"})
    services = {
        c.labels.get("com.docker.compose.service")
        for c in running
        if c.labels.get("com.docker.compose.service")
    }
    missing = sorted(REQUIRED_SERVICES - services)
    if missing:
        raise AirflowException(
            "Required services are not running: " + ", ".join(missing)
        )


def load_batch_and_reference():
    exec_in_service(
        "ingestion",
        [
            "python",
            "/opt/project/hdfs_batch_loader.py",
            "--source-dir",
            BATCH_SOURCE_DIR,
            "--streaming-month",
            STREAMING_MONTH,
            "--reference-dir",
            REFERENCE_DIR,
        ],
    )


def verify_batch_bronze():
    output = exec_in_service(
        "namenode",
        [
            "sh",
            "-lc",
            "hdfs dfs -ls /nyc-taxi/control/batch_loaded 2>/dev/null "
            "| awk '$1 ~ /^d/ {count++} END {print count+0}'",
        ],
    ).strip()

    count = int(output.splitlines()[-1])
    if count != 59:
        raise AirflowException(
            f"Expected 59 batch success markers, found {count}."
        )


def verify_streaming_bronze():
    exec_in_service(
        "namenode",
        ["hdfs", "dfs", "-test", "-e", STREAMING_BRONZE_HDFS],
    )

    output = exec_in_service(
        "namenode",
        [
            "sh",
            "-lc",
            f"hdfs dfs -ls {STREAMING_BRONZE_HDFS} 2>/dev/null "
            "| grep -c '\\.parquet$' || true",
        ],
    ).strip()

    parquet_count = int(output.splitlines()[-1])
    if parquet_count <= 0:
        raise AirflowException(
            "Streaming Bronze directory exists but contains no Parquet files."
        )


def compute_bronze_fingerprint() -> str:
    shell_script = r'''
set -e

echo "[batch_month_markers]"
hdfs dfs -ls /nyc-taxi/control/batch_loaded 2>/dev/null \
  | awk '$1 ~ /^d/ {print $8}' \
  | sort

echo "[streaming_final_offset]"
latest=$(
  hdfs dfs -ls /nyc-taxi/checkpoints/streaming_ingestion/offsets 2>/dev/null \
    | awk '{print $8}' \
    | awk -F/ '{print $NF}' \
    | grep -E '^[0-9]+$' \
    | sort -n \
    | tail -1
)

if [ -n "$latest" ]; then
  hdfs dfs -cat "/nyc-taxi/checkpoints/streaming_ingestion/offsets/$latest" \
    | tail -1
else
  echo "NONE"
fi

echo "[streaming_bronze_size]"
hdfs dfs -du -s /nyc-taxi/bronze/streaming/year=2025/month=12 \
  2>/dev/null || echo "NONE"

echo "[taxi_lookup_checksum]"
hdfs dfs -checksum /nyc-taxi/reference/taxi_zone_lookup.csv \
  2>/dev/null || echo "NONE"
'''

    manifest = exec_in_service(
        "namenode",
        ["sh", "-lc", shell_script],
    )

    fingerprint = hashlib.sha256(
        manifest.encode("utf-8")
    ).hexdigest()

    log.info("Bronze fingerprint: %s", fingerprint)
    return fingerprint


def read_previous_fingerprint() -> str | None:
    output = exec_in_service(
        "namenode",
        [
            "sh",
            "-lc",
            f"if hdfs dfs -test -e {PIPELINE_FINGERPRINT_FILE}; then "
            f"hdfs dfs -cat {PIPELINE_FINGERPRINT_FILE}; fi",
        ],
    ).strip()

    return output.splitlines()[-1].strip() if output else None


def write_fingerprint(fingerprint: str):
    exec_in_service(
        "namenode",
        [
            "sh",
            "-lc",
            f"hdfs dfs -mkdir -p {PIPELINE_STATE_DIR} && "
            f"printf '%s\\n' '{fingerprint}' > /tmp/bronze_fingerprint && "
            f"hdfs dfs -put -f /tmp/bronze_fingerprint "
            f"{PIPELINE_FINGERPRINT_FILE}",
        ],
    )


def completed_outputs_exist() -> bool:
    output = exec_in_service(
        "namenode",
        [
            "sh",
            "-lc",
            "if "
            "hdfs dfs -test -e /nyc-taxi/silver/trips/_SUCCESS && "
            "hdfs dfs -test -e /nyc-taxi/gold/daily_summary/_SUCCESS && "
            "hdfs dfs -test -e /nyc-taxi/gold/hourly_demand/_SUCCESS && "
            "hdfs dfs -test -e /nyc-taxi/gold/pickup_zone_monthly/_SUCCESS && "
            "hdfs dfs -test -e /nyc-taxi/gold/payment_monthly/_SUCCESS; "
            "then echo YES; else echo NO; fi",
        ],
    ).strip()

    return output.splitlines()[-1] == "YES"


def choose_rebuild_path():
    current = compute_bronze_fingerprint()
    previous = read_previous_fingerprint()

    log.info("Previous fingerprint: %s", previous)
    log.info("Current fingerprint : %s", current)

    if previous == current:
        log.info("Bronze unchanged. Skipping Silver and Gold rebuild.")
        return "skip_rebuild"

    if previous is None and completed_outputs_exist():
        log.info(
            "Bootstrapping current completed Silver/Gold state. "
            "No expensive rebuild required."
        )
        write_fingerprint(current)
        return "skip_rebuild"

    return "build_silver"


def build_silver():
    exec_in_service(
        "spark-master",
        [
            "/opt/spark/bin/spark-submit",
            "--master",
            "spark://spark-master:7077",
            "/opt/project/bronze_to_silver.py",
            "--output-path",
            SILVER_PATH,
        ],
        expected_text="BRONZE -> SILVER COMPLETE",
    )


def validate_silver():
    exec_in_service(
        "spark-master",
        [
            "/opt/spark/bin/spark-submit",
            "--master",
            "spark://spark-master:7077",
            "/opt/project/validate_final_silver.py",
        ],
        expected_text="FINAL SILVER VALIDATION COMPLETE",
    )


def build_gold():
    exec_in_service(
        "spark-master",
        [
            "/opt/spark/bin/spark-submit",
            "--master",
            "spark://spark-master:7077",
            "/opt/project/build_gold.py",
            "--output-base",
            GOLD_PATH,
        ],
        expected_text="SILVER -> GOLD COMPLETE",
    )


def validate_gold():
    exec_in_service(
        "spark-master",
        [
            "/opt/spark/bin/spark-submit",
            "--master",
            "spark://spark-master:7077",
            "/opt/project/validate_final_gold.py",
        ],
        expected_text="OVERALL RESULT: PASS",
    )


def record_successful_pipeline_state():
    current = compute_bronze_fingerprint()
    write_fingerprint(current)


default_args = {
    "owner": "nyc-taxi-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="nyc_taxi_batch_to_gold",
    description=(
        "Idempotent NYC Taxi orchestration: rebuild Silver/Gold only when "
        "Bronze/reference state changes."
    ),
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["nyc-taxi", "big-data", "hdfs", "spark", "idempotent"],
) as dag:

    start = EmptyOperator(task_id="start")

    check_services = PythonOperator(
        task_id="check_required_services",
        python_callable=check_required_services,
    )

    batch_ingestion = PythonOperator(
        task_id="load_batch_and_reference",
        python_callable=load_batch_and_reference,
        execution_timeout=timedelta(hours=2),
    )

    check_batch = PythonOperator(
        task_id="verify_batch_bronze",
        python_callable=verify_batch_bronze,
    )

    check_stream = PythonOperator(
        task_id="verify_streaming_bronze",
        python_callable=verify_streaming_bronze,
    )

    bronze_ready = EmptyOperator(task_id="bronze_ready")

    detect_bronze_change = BranchPythonOperator(
        task_id="detect_bronze_change",
        python_callable=choose_rebuild_path,
    )

    skip_rebuild = EmptyOperator(task_id="skip_rebuild")

    silver = PythonOperator(
        task_id="build_silver",
        python_callable=build_silver,
        execution_timeout=timedelta(hours=3),
    )

    silver_validation = PythonOperator(
        task_id="validate_silver",
        python_callable=validate_silver,
        execution_timeout=timedelta(hours=1),
    )

    gold = PythonOperator(
        task_id="build_gold",
        python_callable=build_gold,
        execution_timeout=timedelta(hours=2),
    )

    gold_validation = PythonOperator(
        task_id="validate_gold",
        python_callable=validate_gold,
        execution_timeout=timedelta(hours=1),
    )

    record_state = PythonOperator(
        task_id="record_pipeline_state",
        python_callable=record_successful_pipeline_state,
    )

    complete = EmptyOperator(
        task_id="pipeline_complete",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    start >> check_services

    check_services >> batch_ingestion >> check_batch
    check_services >> check_stream

    [check_batch, check_stream] >> bronze_ready >> detect_bronze_change

    detect_bronze_change >> skip_rebuild >> complete

    (
        detect_bronze_change
        >> silver
        >> silver_validation
        >> gold
        >> gold_validation
        >> record_state
        >> complete
    )
