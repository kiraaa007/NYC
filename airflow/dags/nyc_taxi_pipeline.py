from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timedelta

import docker
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

STREAMING_MONTH = "2025-12"
BATCH_SOURCE_DIR = "/data/trip_records"
REFERENCE_DIR = "/data/taxi_zones"

SILVER_PATH = "hdfs://namenode:9000/nyc-taxi/silver/trips"
GOLD_PATH = "hdfs://namenode:9000/nyc-taxi/gold"
ML_DATASET_BASE = "hdfs://namenode:9000/nyc-taxi/ml/duration_dataset"

STREAMING_BRONZE_HDFS = "/nyc-taxi/bronze/streaming/year=2025/month=12"
PIPELINE_STATE_DIR = "/nyc-taxi/control/pipeline_state"
LEGACY_BRONZE_STATE = f"{PIPELINE_STATE_DIR}/bronze_fingerprint.sha256"
CURRENT_BRONZE_STATE = f"{PIPELINE_STATE_DIR}/bronze_current.sha256"

GOLD_TABLES = [
    "daily_summary",
    "hourly_demand",
    "pickup_zone_monthly",
    "payment_monthly",
]

REQUIRED_SERVICES = {
    "namenode",
    "datanode",
    "kafka",
    "spark-master",
    "spark-worker",
    "ingestion",
}


# ---------------------------------------------------------------------------
# Docker helpers
# ---------------------------------------------------------------------------

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
    chunks = client.api.exec_start(exec_id, stream=True, demux=True)
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
    output = "".join(output_parts)
    if result.get("ExitCode") != 0:
        raise AirflowException(
            f"Command failed in service {service} "
            f"with exit code {result.get('ExitCode')}."
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


def service_file_checksum(service: str, path: str) -> str:
    output = exec_in_service(service, ["sha256sum", path]).strip()
    if not output:
        raise AirflowException(f"Could not checksum {service}:{path}")
    return output.split()[0]


# ---------------------------------------------------------------------------
# Persistent stage state in HDFS
# ---------------------------------------------------------------------------

def fingerprint(*parts: str) -> str:
    return hashlib.sha256("\n".join(parts).encode("utf-8")).hexdigest()


def state_path(stage: str) -> str:
    return f"{PIPELINE_STATE_DIR}/{stage}.sha256"


def read_hdfs_text(path: str) -> str | None:
    output = exec_in_service(
        "namenode",
        [
            "sh",
            "-lc",
            f"if hdfs dfs -test -e {path}; then hdfs dfs -cat {path}; fi",
        ],
    ).strip()
    return output.splitlines()[-1].strip() if output else None


def write_hdfs_text(path: str, value: str):
    temp_name = path.rsplit("/", 1)[-1].replace(".", "_")
    exec_in_service(
        "namenode",
        [
            "sh",
            "-lc",
            f"hdfs dfs -mkdir -p {PIPELINE_STATE_DIR} && "
            f"printf '%s\\n' '{value}' > /tmp/{temp_name} && "
            f"hdfs dfs -put -f /tmp/{temp_name} {path}",
        ],
    )


def shell_check(service: str, condition: str) -> bool:
    output = exec_in_service(
        service,
        ["sh", "-lc", f"if {condition}; then echo YES; else echo NO; fi"],
    ).strip()
    return output.splitlines()[-1] == "YES"


# ---------------------------------------------------------------------------
# Bronze readiness and fingerprint
# ---------------------------------------------------------------------------

def batch_and_reference_complete() -> bool:
    return shell_check(
        "namenode",
        "count=$(hdfs dfs -ls /nyc-taxi/control/batch_loaded 2>/dev/null "
        "| awk '$1 ~ /^d/ {count++} END {print count+0}'); "
        "[ \"$count\" -eq 59 ] && "
        "hdfs dfs -test -e /nyc-taxi/control/reference_loaded/_SUCCESS",
    )


def ensure_batch_and_reference() -> str:
    if batch_and_reference_complete():
        log.info(
            "59 batch months and reference data already exist; "
            "batch/reference ingestion is unchanged."
        )
        return "unchanged"

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

    if not batch_and_reference_complete():
        raise AirflowException(
            "Batch/reference ingestion finished but expected success markers "
            "were not found."
        )
    return "rebuilt"


def verify_streaming_bronze() -> str:
    condition = (
        f"hdfs dfs -test -e {STREAMING_BRONZE_HDFS} && "
        f"[ $(hdfs dfs -ls {STREAMING_BRONZE_HDFS} 2>/dev/null "
        "| grep -c '\\.parquet$' || true) -gt 0 ]"
    )
    if not shell_check("namenode", condition):
        raise AirflowException(
            "Streaming Bronze is missing. Run nyc_taxi_streaming_ingestion "
            "first."
        )
    return "ready"


def compute_bronze_fingerprint() -> str:
    manifest = exec_in_service(
        "namenode",
        [
            "sh",
            "-lc",
            r'''
set -e

echo "[batch_markers]"
for d in $(hdfs dfs -ls /nyc-taxi/control/batch_loaded 2>/dev/null \
  | awk '$1 ~ /^d/ {print $8}' | sort); do
  echo "$d"
  hdfs dfs -cat "$d/_SUCCESS" 2>/dev/null || echo MISSING
done

echo "[batch_size]"
hdfs dfs -du -s /nyc-taxi/bronze/batch 2>/dev/null || echo NONE

echo "[batch_file_checksums]"
for f in $(hdfs dfs -find /nyc-taxi/bronze/batch -name '*.parquet' 2>/dev/null | sort); do
  hdfs dfs -checksum "$f" 2>/dev/null || echo "MISSING:$f"
done

echo "[stream_checkpoint]"
latest=$(hdfs dfs -ls /nyc-taxi/checkpoints/streaming_ingestion/offsets \
  2>/dev/null | awk '{print $8}' | awk -F/ '{print $NF}' \
  | grep -E '^[0-9]+$' | sort -n | tail -1)
if [ -n "$latest" ]; then
  echo "batch=$latest"
  hdfs dfs -cat "/nyc-taxi/checkpoints/streaming_ingestion/offsets/$latest" \
    | tail -1
  if hdfs dfs -test -e "/nyc-taxi/checkpoints/streaming_ingestion/commits/$latest"; then
    echo "commit=YES"
  else
    echo "commit=NO"
  fi
else
  echo NONE
fi

echo "[stream_size]"
hdfs dfs -du -s /nyc-taxi/bronze/streaming/year=2025/month=12 \
  2>/dev/null || echo NONE

echo "[stream_file_checksums]"
for f in $(hdfs dfs -find /nyc-taxi/bronze/streaming/year=2025/month=12 \
  -name '*.parquet' 2>/dev/null | sort); do
  hdfs dfs -checksum "$f" 2>/dev/null || echo "MISSING:$f"
done

echo "[lookup_checksum]"
hdfs dfs -checksum /nyc-taxi/reference/taxi_zone_lookup.csv \
  2>/dev/null || echo NONE

echo "[zone_reference_checksums]"
for f in $(hdfs dfs -ls /nyc-taxi/reference/taxi_zones 2>/dev/null \
  | awk '$1 !~ /^d/ && $8 != "" {print $8}' | sort); do
  hdfs dfs -checksum "$f" 2>/dev/null || echo "MISSING:$f"
done
''',
        ],
    )
    value = fingerprint(manifest)
    log.info("Bronze fingerprint: %s", value)
    return value


def snapshot_bronze_state() -> str:
    value = compute_bronze_fingerprint()
    write_hdfs_text(CURRENT_BRONZE_STATE, value)
    # Keep the old control-file name for compatibility with earlier runs.
    write_hdfs_text(LEGACY_BRONZE_STATE, value)
    return value


# ---------------------------------------------------------------------------
# Idempotent downstream stages
# ---------------------------------------------------------------------------
# Every stage fingerprints BOTH its upstream data state and the code that
# creates it. If outputs already exist and no stage fingerprint exists yet,
# we bootstrap the fingerprint without rebuilding the existing project.
STAGES = {
    "silver": {
        "parent": "bronze",
        "code": [
            ("spark-master", "/opt/project/bronze_to_silver.py"),
            ("spark-master", "/opt/project/validate_final_silver.py"),
        ],
        "output": (
            "namenode",
            "hdfs dfs -test -e /nyc-taxi/silver/trips/_SUCCESS",
        ),
        "build": (
            "spark-master",
            [
                "/opt/spark/bin/spark-submit",
                "--master",
                "spark://spark-master:7077",
                "/opt/project/bronze_to_silver.py",
                "--output-path",
                SILVER_PATH,
            ],
            "BRONZE -> SILVER COMPLETE",
        ),
        "validate": (
            "spark-master",
            [
                "/opt/spark/bin/spark-submit",
                "--master",
                "spark://spark-master:7077",
                "/opt/project/validate_final_silver.py",
            ],
            "FINAL SILVER VALIDATION COMPLETE",
        ),
    },
    "gold": {
        "parent": "silver",
        "code": [
            ("spark-master", "/opt/project/build_gold.py"),
            ("spark-master", "/opt/project/validate_final_gold.py"),
        ],
        "output": (
            "namenode",
            " && ".join(
                f"hdfs dfs -test -e /nyc-taxi/gold/{t}/_SUCCESS"
                for t in GOLD_TABLES
            ),
        ),
        "build": (
            "spark-master",
            [
                "/opt/spark/bin/spark-submit",
                "--master",
                "spark://spark-master:7077",
                "/opt/project/build_gold.py",
                "--output-base",
                GOLD_PATH,
            ],
            "SILVER -> GOLD COMPLETE",
        ),
        "validate": (
            "spark-master",
            [
                "/opt/spark/bin/spark-submit",
                "--master",
                "spark://spark-master:7077",
                "/opt/project/validate_final_gold.py",
            ],
            "OVERALL RESULT: PASS",
        ),
    },
    "dashboard_export": {
        "parent": "gold",
        "code": [("spark-master", "/opt/project/export_dashboard_data.py")],
        "output": (
            "spark-master",
            " && ".join(
                "[ -n \"$(find /opt/dashboard-data/"
                f"{t} -maxdepth 1 -type f -name '*.parquet' -print -quit "
                "2>/dev/null)\" ]"
                for t in GOLD_TABLES
            ),
        ),
        "build": (
            "spark-master",
            [
                "/opt/spark/bin/spark-submit",
                "--master",
                "spark://spark-master:7077",
                "/opt/project/export_dashboard_data.py",
            ],
            "DASHBOARD DATA EXPORT COMPLETE",
        ),
    },
    "ml_dataset": {
        "parent": "silver",
        "code": [
            ("spark-master", "/opt/project/build_duration_ml_dataset.py")
        ],
        "output": (
            "namenode",
            "hdfs dfs -test -e /nyc-taxi/ml/duration_dataset/train/_SUCCESS "
            "&& hdfs dfs -test -e /nyc-taxi/ml/duration_dataset/test/_SUCCESS",
        ),
        "build": (
            "spark-master",
            [
                "/opt/spark/bin/spark-submit",
                "--master",
                "spark://spark-master:7077",
                "/opt/project/build_duration_ml_dataset.py",
                "--output-base",
                ML_DATASET_BASE,
            ],
            "ML DATASET BUILD COMPLETE",
        ),
    },
    "spark_models": {
        "parent": "ml_dataset",
        "code": [("spark-master", "/opt/project/train_duration_models.py")],
        "output": (
            "namenode",
            "hdfs dfs -test -e /nyc-taxi/ml/models/duration/linear/metadata "
            "&& hdfs dfs -test -e /nyc-taxi/ml/models/duration/rf/metadata "
            "&& hdfs dfs -test -e /nyc-taxi/ml/models/duration/gbt/metadata "
            "&& hdfs dfs -test -e "
            "/nyc-taxi/ml/metrics/duration_model_comparison/_SUCCESS "
            "&& hdfs dfs -test -e "
            "/nyc-taxi/ml/predictions/duration/2025-12/_SUCCESS",
        ),
        "build": (
            "spark-master",
            [
                "/opt/spark/bin/spark-submit",
                "--master",
                "spark://spark-master:7077",
                "/opt/project/train_duration_models.py",
            ],
            "TRIP DURATION MODEL TRAINING COMPLETE",
        ),
    },
    "ml_export": {
        "parent": "ml_dataset",
        "code": [
            (
                "spark-master",
                "/opt/project/export_ml_dataset_for_deployment.py",
            )
        ],
        "output": (
            "spark-master",
            "[ -n \"$(find /opt/ml-artifacts/data/train -maxdepth 1 -type f "
            "-name '*.parquet' -print -quit 2>/dev/null)\" ] "
            "&& [ -n \"$(find /opt/ml-artifacts/data/test -maxdepth 1 -type f "
            "-name '*.parquet' -print -quit 2>/dev/null)\" ]",
        ),
        "build": (
            "spark-master",
            [
                "/opt/spark/bin/spark-submit",
                "--master",
                "spark://spark-master:7077",
                "/opt/project/export_ml_dataset_for_deployment.py",
            ],
            "ML DEPLOYMENT DATA EXPORT COMPLETE",
        ),
    },
    "portable_model": {
        "parent": "ml_export",
        "code": [
            ("spark-master", "/opt/ml-training/train_deployable_model.py"),
            ("spark-master", "/opt/ml-training/requirements.txt"),
        ],
        "output": (
            "spark-master",
            "[ -s /opt/ml-artifacts/trip_duration_model.pkl ] "
            "&& [ -s /opt/ml-artifacts/trip_duration_model_metrics.json ] "
            "&& [ -s /opt/ml-artifacts/trip_duration_model_features.json ]",
        ),
        "build": (
            "ml-trainer",
            ["python", "/trainer/train_deployable_model.py"],
            "PORTABLE MODEL TRAINING COMPLETE",
        ),
    },
}


def desired_stage_fingerprint(stage: str) -> str:
    if stage == "bronze":
        current = read_hdfs_text(CURRENT_BRONZE_STATE)
        if current is None:
            current = snapshot_bronze_state()
        return current

    spec = STAGES[stage]
    parent_fp = desired_stage_fingerprint(spec["parent"])
    code_fps = [
        service_file_checksum(service, path)
        for service, path in spec["code"]
    ]
    return fingerprint(f"{stage}-v3", parent_fp, *code_fps)


def stage_output_exists(stage: str) -> bool:
    service, condition = STAGES[stage]["output"]
    return shell_check(service, condition)


def execute_command(spec):
    service, command, expected_text = spec
    exec_in_service(service, command, expected_text=expected_text)


def ensure_stage(stage: str) -> str:
    desired = desired_stage_fingerprint(stage)
    previous = read_hdfs_text(state_path(stage))
    exists = stage_output_exists(stage)

    log.info("%s previous fingerprint: %s", stage, previous)
    log.info("%s desired fingerprint : %s", stage, desired)
    log.info("%s outputs exist       : %s", stage, exists)

    if exists and previous == desired:
        log.info("%s is unchanged; existing output will be reused.", stage)
        return "unchanged"

    # Critical first-run behavior: do not rebuild the already-finished project
    # just because the new orchestration state files do not exist yet.
    if exists and previous is None:
        write_hdfs_text(state_path(stage), desired)
        log.info(
            "%s bootstrapped from the existing output WITHOUT rebuilding.",
            stage,
        )
        return "bootstrapped"

    reason = "output missing" if not exists else "input/code changed"
    log.info("%s will rebuild because %s.", stage, reason)

    spec = STAGES[stage]
    execute_command(spec["build"])
    if spec.get("validate"):
        execute_command(spec["validate"])

    if not stage_output_exists(stage):
        raise AirflowException(
            f"{stage} finished, but its expected output was not found."
        )

    write_hdfs_text(state_path(stage), desired)

    log.info("%s rebuilt successfully.", stage)
    return "rebuilt"


def refresh_streamlit_if_needed(**context) -> str:
    statuses = context["ti"].xcom_pull(
        task_ids=["ensure_dashboard_export", "ensure_portable_model"]
    ) or []
    if not any(status == "rebuilt" for status in statuses):
        log.info("Serving artifacts unchanged; Streamlit restart not needed.")
        return "unchanged"

    client = docker_client()
    containers = client.containers.list(
        filters={
            "label": "com.docker.compose.service=streamlit-dashboard",
            "status": "running",
        }
    )
    if not containers:
        log.info("Local Streamlit is not running; nothing to restart.")
        return "not_running"

    containers[0].restart(timeout=15)
    log.info("Local Streamlit restarted to load new serving artifacts.")
    return "restarted"


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
default_args = {
    "owner": "nyc-taxi-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="nyc_taxi_full_pipeline",
    description=(
        "End-to-end idempotent NYC Taxi pipeline. Each stage checks its "
        "upstream fingerprint, code checksum and existing output before "
        "deciding whether to rebuild."
    ),
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["nyc-taxi", "big-data", "spark", "ml", "idempotent"],
) as dag:
    start = EmptyOperator(task_id="start")
    check_services = PythonOperator(
        task_id="check_required_services",
        python_callable=check_required_services,
    )
    batch_ready = PythonOperator(
        task_id="ensure_batch_and_reference",
        python_callable=ensure_batch_and_reference,
        execution_timeout=timedelta(hours=2),
    )
    stream_ready = PythonOperator(
        task_id="verify_streaming_bronze",
        python_callable=verify_streaming_bronze,
    )
    bronze_ready = EmptyOperator(task_id="bronze_ready")
    bronze_snapshot = PythonOperator(
        task_id="snapshot_bronze_fingerprint",
        python_callable=snapshot_bronze_state,
    )

    silver_ready = PythonOperator(
        task_id="ensure_silver",
        python_callable=ensure_stage,
        op_args=["silver"],
        execution_timeout=timedelta(hours=4),
    )
    gold_ready = PythonOperator(
        task_id="ensure_gold",
        python_callable=ensure_stage,
        op_args=["gold"],
        execution_timeout=timedelta(hours=3),
    )
    dashboard_ready = PythonOperator(
        task_id="ensure_dashboard_export",
        python_callable=ensure_stage,
        op_args=["dashboard_export"],
        execution_timeout=timedelta(hours=1),
    )
    ml_dataset_ready = PythonOperator(
        task_id="ensure_ml_dataset",
        python_callable=ensure_stage,
        op_args=["ml_dataset"],
        execution_timeout=timedelta(hours=2),
    )
    spark_models_ready = PythonOperator(
        task_id="ensure_spark_models",
        python_callable=ensure_stage,
        op_args=["spark_models"],
        execution_timeout=timedelta(hours=2),
    )
    ml_export_ready = PythonOperator(
        task_id="ensure_ml_export",
        python_callable=ensure_stage,
        op_args=["ml_export"],
        execution_timeout=timedelta(hours=1),
    )
    portable_model_ready = PythonOperator(
        task_id="ensure_portable_model",
        python_callable=ensure_stage,
        op_args=["portable_model"],
        execution_timeout=timedelta(hours=1),
    )
    refresh_streamlit = PythonOperator(
        task_id="refresh_local_streamlit",
        python_callable=refresh_streamlit_if_needed,
    )
    complete = EmptyOperator(task_id="pipeline_complete")

    start >> check_services
    check_services >> batch_ready
    check_services >> stream_ready
    [batch_ready, stream_ready] >> bronze_ready >> bronze_snapshot

    (
        bronze_snapshot
        >> silver_ready
        >> gold_ready
        >> dashboard_ready
        >> ml_dataset_ready
        >> spark_models_ready
        >> ml_export_ready
        >> portable_model_ready
        >> refresh_streamlit
        >> complete
    )
