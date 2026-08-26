
from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta

import docker
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

STREAMING_MONTH = "2025-12"
TOPIC = "nyc_taxi_stream"
SOURCE_FILE = "/data/trip_records/yellow_tripdata_2025-12.parquet"

BRONZE_PATH = "/nyc-taxi/bronze/streaming/year=2025/month=12"
CHECKPOINT_ROOT = "/nyc-taxi/checkpoints/streaming_ingestion"

SUCCESS_DIR = "/nyc-taxi/control/streaming_loaded/2025-12"
SUCCESS_MARKER = f"{SUCCESS_DIR}/_SUCCESS"

CONSUMER_PID_FILE = "/tmp/nyc_streaming_airflow.pid"
CONSUMER_LOG_FILE = "/tmp/nyc_streaming_airflow.log"

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


def exec_in_service(service: str, command: list[str]) -> str:
    client, container = find_service_container(service)

    exec_info = client.api.exec_create(
        container=container.id,
        cmd=command,
        stdout=True,
        stderr=True,
    )
    exec_id = exec_info["Id"]

    chunks = client.api.exec_start(exec_id, stream=True, demux=True)
    output_parts = []

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

    return output


def check_required_services():
    client = docker_client()
    services = {
        c.labels.get("com.docker.compose.service")
        for c in client.containers.list(filters={"status": "running"})
        if c.labels.get("com.docker.compose.service")
    }

    missing = sorted(REQUIRED_SERVICES - services)
    if missing:
        raise AirflowException(
            "Required services are not running: " + ", ".join(missing)
        )


def ensure_kafka_topic():
    exec_in_service(
        "kafka",
        [
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            "kafka:9092",
            "--create",
            "--if-not-exists",
            "--topic",
            TOPIC,
            "--partitions",
            "3",
            "--replication-factor",
            "1",
        ],
    )


def expected_source_rows() -> int:
    output = exec_in_service(
        "ingestion",
        [
            "python",
            "-c",
            (
                "import pyarrow.parquet as pq; "
                f"print(pq.ParquetFile('{SOURCE_FILE}').metadata.num_rows)"
            ),
        ],
    ).strip()
    return int(output.splitlines()[-1])


def kafka_total_end_offset() -> int:
    output = exec_in_service(
        "kafka",
        [
            "/opt/kafka/bin/kafka-get-offsets.sh",
            "--bootstrap-server",
            "kafka:9092",
            "--topic",
            TOPIC,
        ],
    )

    total = 0
    for line in output.splitlines():
        parts = line.strip().split(":")
        if len(parts) == 3:
            total += int(parts[2])
    return total


def streaming_bronze_file_count() -> int:
    output = exec_in_service(
        "namenode",
        [
            "sh",
            "-lc",
            (
                f"hdfs dfs -ls {BRONZE_PATH} 2>/dev/null "
                "| grep -c '\\.parquet$' || true"
            ),
        ],
    ).strip()
    return int(output.splitlines()[-1])


def latest_checkpoint_batch() -> int | None:
    output = exec_in_service(
        "namenode",
        [
            "sh",
            "-lc",
            (
                f"hdfs dfs -ls {CHECKPOINT_ROOT}/offsets 2>/dev/null "
                "| awk '{print $8}' "
                "| awk -F/ '{print $NF}' "
                "| grep -E '^[0-9]+$' "
                "| sort -n "
                "| tail -1 || true"
            ),
        ],
    ).strip()

    return int(output.splitlines()[-1]) if output else None


def checkpoint_total_offset(batch_id: int | None) -> int:
    if batch_id is None:
        return 0

    output = exec_in_service(
        "namenode",
        ["hdfs", "dfs", "-cat", f"{CHECKPOINT_ROOT}/offsets/{batch_id}"],
    )

    json_lines = [
        line.strip()
        for line in output.splitlines()
        if line.strip().startswith("{")
    ]

    if not json_lines:
        return 0

    payload = json.loads(json_lines[-1])
    topic_offsets = payload.get(TOPIC, {})
    return sum(int(v) for v in topic_offsets.values())


def commit_exists(batch_id: int | None) -> bool:
    if batch_id is None:
        return False

    output = exec_in_service(
        "namenode",
        [
            "sh",
            "-lc",
            (
                f"if hdfs dfs -test -e {CHECKPOINT_ROOT}/commits/{batch_id}; "
                "then echo YES; else echo NO; fi"
            ),
        ],
    ).strip()

    return output.splitlines()[-1] == "YES"


def success_marker_exists() -> bool:
    output = exec_in_service(
        "namenode",
        [
            "sh",
            "-lc",
            (
                f"if hdfs dfs -test -e {SUCCESS_MARKER}; "
                "then echo YES; else echo NO; fi"
            ),
        ],
    ).strip()

    return output.splitlines()[-1] == "YES"


def stream_state() -> dict:
    rows = expected_source_rows()
    expected_messages = rows + 1
    kafka_end = kafka_total_end_offset()
    bronze_files = streaming_bronze_file_count()
    batch_id = latest_checkpoint_batch()
    spark_end = checkpoint_total_offset(batch_id)
    committed = commit_exists(batch_id)

    state = {
        "source_rows": rows,
        "expected_messages": expected_messages,
        "kafka_end": kafka_end,
        "bronze_files": bronze_files,
        "checkpoint_batch": batch_id,
        "spark_end": spark_end,
        "committed": committed,
    }

    log.info("Streaming state: %s", state)
    return state


def hdfs_stream_is_complete(state: dict) -> bool:
    """
    Durable completion criterion.

    Kafka broker logs may be ephemeral and can reset after a Docker restart.
    HDFS Bronze plus the Spark checkpoint/commit are the durable proof that
    the finite source was already consumed.
    """
    return (
        state["spark_end"] == state["expected_messages"]
        and state["committed"]
        and state["bronze_files"] > 0
    )


def active_stream_is_complete(state: dict) -> bool:
    """
    Strong criterion used while actively ingesting a fresh stream.
    """
    return (
        hdfs_stream_is_complete(state)
        and state["kafka_end"] == state["expected_messages"]
    )


def write_success_marker(state: dict, status: str):
    metadata = (
        f"month={STREAMING_MONTH}\n"
        f"status={status}\n"
        f"source_rows={state['source_rows']}\n"
        f"expected_kafka_messages={state['expected_messages']}\n"
        f"kafka_end_offset_at_validation={state['kafka_end']}\n"
        f"spark_checkpoint_offset_total={state['spark_end']}\n"
        f"checkpoint_batch={state['checkpoint_batch']}\n"
        f"bronze_files={state['bronze_files']}\n"
        f"committed={state['committed']}\n"
    )

    escaped = metadata.replace("'", "'\"'\"'")

    exec_in_service(
        "namenode",
        [
            "sh",
            "-lc",
            (
                f"hdfs dfs -mkdir -p {SUCCESS_DIR} && "
                f"printf '%s' '{escaped}' > /tmp/stream_success && "
                f"hdfs dfs -put -f /tmp/stream_success {SUCCESS_MARKER}"
            ),
        ],
    )


def choose_streaming_path():
    if success_marker_exists():
        log.info(
            "Streaming success marker already exists. "
            "Producer replay is not required."
        )
        return "streaming_already_complete"

    state = stream_state()

    # Normal case: broker still has the original messages.
    if active_stream_is_complete(state):
        log.info(
            "Kafka, Spark checkpoint, commit and HDFS Bronze all show "
            "a completed stream. Creating success marker."
        )
        write_success_marker(state, "complete")
        return "streaming_already_complete"

    # Recovery case: Kafka broker storage reset, but Spark/HDFS durable state
    # proves that all 4,305,007 messages were already committed.
    if hdfs_stream_is_complete(state) and state["kafka_end"] == 0:
        log.info(
            "Spark/HDFS prove the stream completed, while Kafka end offset "
            "is now 0. Treating this as completed ingestion after Kafka "
            "broker-state reset. Producer replay is blocked."
        )
        write_success_marker(state, "complete_kafka_broker_reset")
        return "streaming_already_complete"

    # If HDFS says complete but Kafka is partially populated, do not replay
    # or silently accept the state. It needs manual inspection.
    if hdfs_stream_is_complete(state):
        raise AirflowException(
            "Spark/HDFS show completed ingestion, but Kafka currently has "
            f"{state['kafka_end']} messages instead of 0 or "
            f"{state['expected_messages']}. Manual Kafka inspection is "
            "required; producer replay was blocked."
        )

    fresh_state = (
        state["kafka_end"] == 0
        and state["bronze_files"] == 0
        and state["checkpoint_batch"] is None
        and state["spark_end"] == 0
    )

    if fresh_state:
        return "start_streaming_consumer"

    raise AirflowException(
        "Partial or inconsistent streaming state detected. "
        "Producer replay was blocked to prevent duplicate records."
    )


def start_streaming_consumer():
    exec_in_service(
        "spark-master",
        [
            "sh",
            "-lc",
            (
                "mkdir -p /tmp/.ivy2; "
                f"rm -f {CONSUMER_PID_FILE}; "
                f"rm -f {CONSUMER_LOG_FILE}; "
                "nohup /opt/spark/bin/spark-submit "
                "--master spark://spark-master:7077 "
                "--conf spark.jars.ivy=/tmp/.ivy2 "
                "--packages "
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 "
                "/opt/project/spark_streaming_consumer.py "
                f"--streaming-month {STREAMING_MONTH} "
                f"> {CONSUMER_LOG_FILE} 2>&1 & "
                f"echo $! > {CONSUMER_PID_FILE}; "
                "sleep 10; "
                f"pid=$(cat {CONSUMER_PID_FILE}); "
                "if ! kill -0 \"$pid\" 2>/dev/null; then "
                f"cat {CONSUMER_LOG_FILE}; "
                "exit 1; "
                "fi"
            ),
        ],
    )


def run_kafka_producer():
    exec_in_service(
        "ingestion",
        [
            "python",
            "/opt/project/kafka_streaming_producer.py",
            "--file",
            SOURCE_FILE,
            "--month",
            STREAMING_MONTH,
            "--delay",
            "0",
        ],
    )


def wait_for_streaming_completion():
    expected_messages = expected_source_rows() + 1
    deadline = time.time() + (2 * 60 * 60)

    while time.time() < deadline:
        state = stream_state()

        log.info(
            "Progress: Kafka=%d/%d Spark=%d/%d batch=%s "
            "committed=%s BronzeFiles=%d",
            state["kafka_end"],
            expected_messages,
            state["spark_end"],
            expected_messages,
            state["checkpoint_batch"],
            state["committed"],
            state["bronze_files"],
        )

        if active_stream_is_complete(state):
            return

        if (
            state["kafka_end"] > expected_messages
            or state["spark_end"] > expected_messages
        ):
            raise AirflowException(
                "Observed offset exceeded expected message count. "
                "Possible Kafka replay/duplication."
            )

        time.sleep(10)

    raise AirflowException(
        "Timed out waiting for Spark to commit the final Kafka offset."
    )


def stop_streaming_consumer():
    exec_in_service(
        "spark-master",
        [
            "sh",
            "-lc",
            (
                f"if [ -f {CONSUMER_PID_FILE} ]; then "
                f"pid=$(cat {CONSUMER_PID_FILE}); "
                "kill \"$pid\" 2>/dev/null || true; "
                "sleep 5; "
                "kill -9 \"$pid\" 2>/dev/null || true; "
                f"rm -f {CONSUMER_PID_FILE}; "
                "fi; true"
            ),
        ],
    )


def mark_streaming_success():
    state = stream_state()

    if not active_stream_is_complete(state):
        raise AirflowException(
            "Fresh streaming run did not reach the expected final Kafka "
            "and Spark offsets. Success marker will not be created."
        )

    write_success_marker(state, "complete")


default_args = {
    "owner": "nyc-taxi-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="nyc_taxi_streaming_ingestion",
    description=(
        "Idempotent finite Kafka -> Spark Structured Streaming -> "
        "HDFS Bronze ingestion for NYC Yellow Taxi 2025-12."
    ),
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["nyc-taxi", "kafka", "spark-streaming", "hdfs"],
) as dag:

    start = EmptyOperator(task_id="start")

    check_services = PythonOperator(
        task_id="check_required_services",
        python_callable=check_required_services,
    )

    ensure_topic = PythonOperator(
        task_id="ensure_kafka_topic",
        python_callable=ensure_kafka_topic,
    )

    check_state = BranchPythonOperator(
        task_id="check_existing_streaming_state",
        python_callable=choose_streaming_path,
    )

    already_complete = EmptyOperator(
        task_id="streaming_already_complete"
    )

    start_consumer = PythonOperator(
        task_id="start_streaming_consumer",
        python_callable=start_streaming_consumer,
    )

    producer = PythonOperator(
        task_id="run_kafka_producer",
        python_callable=run_kafka_producer,
        execution_timeout=timedelta(hours=2),
    )

    wait_complete = PythonOperator(
        task_id="wait_for_streaming_completion",
        python_callable=wait_for_streaming_completion,
        execution_timeout=timedelta(hours=2),
    )

    stop_consumer = PythonOperator(
        task_id="stop_streaming_consumer",
        python_callable=stop_streaming_consumer,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    mark_success = PythonOperator(
        task_id="mark_streaming_success",
        python_callable=mark_streaming_success,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    complete = EmptyOperator(
        task_id="streaming_pipeline_complete",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    start >> check_services >> ensure_topic >> check_state

    check_state >> already_complete >> complete

    check_state >> start_consumer >> producer >> wait_complete

    [producer, wait_complete] >> stop_consumer
    [wait_complete, stop_consumer] >> mark_success >> complete
