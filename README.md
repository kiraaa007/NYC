# NYC Yellow Taxi Big Data Analytics

A hybrid **batch + streaming Big Data project** built on five years of NYC TLC Yellow Taxi data, covering **January 2021 through December 2025 (60 months)**.

The project combines Hadoop HDFS, Apache Kafka, Apache Spark, Spark Structured Streaming, Apache Airflow, machine learning, Docker, and Streamlit to build an end-to-end platform from raw taxi trip files to analytics, geospatial visualization, and trip-duration prediction.

## Live Project

- **Streamlit App:** https://ax8afezqcq6m6mbezghqhj.streamlit.app/
- **GitHub Repository:** https://github.com/kiraaa007/NYC

---

## Architecture

```text
NYC Yellow Taxi monthly Parquet files
2021-01 -> 2025-12
                |
        +-------+--------+
        |                |
  59 Batch Months    2025-12 Streaming Month
        |                |
        |          Kafka Producer
        |                |
        |              Kafka
        |                |
        |       Spark Structured Streaming
        |                |
        v                v
 HDFS Bronze Batch   HDFS Bronze Streaming
        |                |
        +-------+--------+
                |
                v
        Spark Bronze -> Silver
        - schema normalization
        - conservative cleaning
        - batch/stream union
        - taxi-zone enrichment
        - derived time/duration fields
                |
                v
          HDFS Silver Trips
                |
        +-------+------------------+
        |                          |
        v                          v
  Spark Silver -> Gold       ML Dataset Builder
        |                          |
        v                          v
  Gold serving tables       Spark model comparison
        |                          |
        v                          v
 Dashboard export            ML train/test export
        |                          |
        |                          v
        |                  Portable .pkl model
        |                          |
        +-------------+------------+
                      |
                      v
                  Streamlit
       analytics + map + ML prediction
```

The full taxi dataset remains in HDFS. Streamlit only consumes compact Gold exports, taxi-zone reference files, and the portable model artifact.

---

## Data Scope

- **Dataset:** NYC TLC Yellow Taxi trip records
- **Period:** January 2021 through December 2025
- **Total months:** 60
- **Batch months:** 59
- **Streaming month:** December 2025 (`2025-12`)
- **Final Silver rows:** 198,091,763 in the completed project state

December 2025 is intentionally excluded from normal batch ingestion and is simulated as a finite stream through Kafka.

---

## Docker Services

`docker-compose.yml` provides the local Big Data environment:

- `namenode` — HDFS NameNode
- `datanode` — HDFS DataNode
- `kafka` — Kafka broker/controller in KRaft mode
- `kafka-init` — creates `nyc_taxi_stream`
- `spark-master` — Spark master and job submission container
- `spark-worker` — Spark worker
- `ingestion` — batch loader + Kafka producer
- `airflow-postgres` — Airflow metadata database
- `airflow-webserver` — Airflow UI
- `airflow-scheduler` — executes the DAGs through Docker
- `ml-trainer` — one-shot portable-model trainer under the `training` profile
- `streamlit-dashboard` — local analytics and prediction UI

HDFS data is persisted in Docker volumes. Do **not** use `docker compose down -v` unless you intentionally want to delete the HDFS state.

---

## Ingestion

### Batch ingestion

`ingestion/hdfs_batch_loader.py`

- discovers the monthly Yellow Taxi Parquet files
- excludes `2025-12`
- uploads the other 59 months to HDFS Bronze
- uploads Taxi Zone Lookup + shapefile reference files
- creates `_SUCCESS` markers per loaded month
- skips months/reference data that are already complete

Batch layout:

```text
/nyc-taxi/bronze/batch/
  year=YYYY/
    month=MM/
```

Reference data:

```text
/nyc-taxi/reference/
```

### Streaming ingestion

`ingestion/kafka_streaming_producer.py`

Reads the December 2025 Parquet file incrementally and publishes one taxi trip per Kafka message to:

```text
nyc_taxi_stream
```

It sends one final EOF control message after all trips are published.

`spark/spark_streaming_consumer.py`

Consumes Kafka with Spark Structured Streaming and writes raw streaming records to:

```text
/nyc-taxi/bronze/streaming/year=2025/month=12
```

The consumer preserves raw JSON plus Kafka partition, offset, and timestamp metadata. Cleaning is intentionally deferred to Silver.

---

## Airflow Orchestration

There are two Airflow DAGs.

### 1. `nyc_taxi_streaming_ingestion`

Defined in:

```text
airflow/dags/nyc_taxi_streaming_pipeline.py
```

It manages the finite December 2025 Kafka ingestion:

```text
check services
-> ensure Kafka topic
-> inspect existing Kafka/Spark/HDFS state
-> start Spark streaming consumer only if needed
-> run Kafka producer only if needed
-> wait for final committed offset
-> stop consumer
-> write streaming success marker
```

The DAG is idempotent. It also handles the case where Kafka broker state was reset but the durable Spark checkpoint and HDFS Bronze output prove that the stream had already completed. In that case, replay is blocked to avoid duplicates.

### 2. `nyc_taxi_full_pipeline`

Defined in:

```text
airflow/dags/nyc_taxi_pipeline.py
```

This is the main end-to-end orchestration DAG:

```text
check services
-> ensure batch + reference data
-> verify streaming Bronze
-> snapshot Bronze fingerprint
-> ensure Silver
-> ensure Gold
-> ensure dashboard export
-> ensure ML dataset
-> ensure Spark model comparison
-> ensure ML train/test export
-> ensure portable .pkl model
-> refresh local Streamlit only if serving artifacts changed
-> complete
```

### Stage-by-stage idempotency

The full DAG is specifically designed **not to rebuild the completed project every time it runs**.

For each downstream stage Airflow records a fingerprint in:

```text
/nyc-taxi/control/pipeline_state/
```

Examples:

```text
silver.sha256
gold.sha256
dashboard_export.sha256
ml_dataset.sha256
spark_models.sha256
ml_export.sha256
portable_model.sha256
```

A stage fingerprint is based on its upstream data fingerprint plus the checksum/version of the code that produces that stage.

Before executing a stage, Airflow asks:

```text
1. Does the expected output already exist?
2. Is there a saved fingerprint for this stage?
3. Does the current fingerprint match the previous fingerprint?
```

Behavior:

```text
output exists + fingerprint unchanged
    -> reuse existing output
    -> NO rebuild

output exists + no stage fingerprint yet
    -> bootstrap the new state file
    -> NO rebuild

output missing
    -> rebuild that stage

upstream data/code fingerprint changed
    -> rebuild that stage
    -> downstream dependent stages detect the new fingerprint themselves
```

This bootstrap rule is important because the project was already completed before the full orchestration logic was added. The first run of the new DAG records the current state instead of unnecessarily rebuilding hundreds of millions of rows.

The Bronze fingerprint includes HDFS batch/streaming state, success markers, HDFS file checksums, the Spark streaming checkpoint/commit, and taxi-zone reference checksums.

### Change propagation examples

If nothing changed:

```text
Silver        reuse
Gold          reuse
Dashboard     reuse
ML dataset    reuse
Spark models  reuse
ML export     reuse
.pkl model    reuse
```

If only `build_gold.py` changes:

```text
Silver        reuse
Gold          rebuild
Dashboard     rebuild
ML dataset    reuse
Spark models  reuse
ML export     reuse
.pkl model    reuse
```

If Bronze/Silver changes:

```text
Silver        rebuild
Gold          rebuild
Dashboard     rebuild
ML dataset    rebuild
Spark models  rebuild
ML export     rebuild
.pkl model    rebuild
```

If only the dashboard-export code changes, only the serving export is regenerated.

---

## Silver Layer

`spark/bronze_to_silver.py`

The job:

1. reads every batch month independently to handle TLC schema drift
2. parses the streaming Bronze raw JSON
3. normalizes batch + streaming records into one canonical schema
4. unions both ingestion paths
5. applies conservative structural cleaning
6. calculates trip-duration and pickup-time features
7. joins the Taxi Zone Lookup for pickup and dropoff enrichment
8. writes partitioned Silver Parquet

Output:

```text
/nyc-taxi/silver/trips/
  pickup_year=YYYY/
    pickup_month=MM/
```

Silver preserves an `ingestion_source` field so batch and streaming records remain traceable after the union.

---

## Gold Layer

`spark/build_gold.py`

Builds compact analytics tables:

- `daily_summary`
- `hourly_demand`
- `pickup_zone_monthly`
- `payment_monthly`

These tables contain metrics such as trip count, revenue, average fare, average tip, trip distance, trip duration, and passenger count.

`spark/validate_final_gold.py` reconciles each Gold table against the **current Silver output**, rather than relying on a hard-coded historical row count. This allows legitimate future Silver changes to be validated correctly.

---

## Machine Learning

Target:

```text
trip_duration_minutes
```

### ML dataset

`spark/build_duration_ml_dataset.py`

Temporal split:

```text
Train: 2021-01 through 2025-11
Test : 2025-12
```

The features include pickup/dropoff location, passenger count, trip distance, rate code, time fields, weekend/rush-hour flags, and cyclical hour/month features.

### Spark model comparison

`spark/train_duration_models.py`

Compares:

- mean baseline
- Linear Regression
- Random Forest
- Gradient-Boosted Trees

Evaluation metrics:

- MAE
- RMSE
- R²

### Portable deployment model

Spark ML models require a Spark/JVM runtime, so deployment uses a separate portable model trained by:

```text
ml_training/train_deployable_model.py
```

It creates:

```text
ml_artifacts/trip_duration_model.pkl
ml_artifacts/trip_duration_model_metrics.json
ml_artifacts/trip_duration_model_features.json
```

Current portable-model metrics:

- Training rows: **841,051**
- Test rows: **143,109**
- Test period: **2025-12**
- MAE: **4.415 min**
- RMSE: **6.996 min**
- R²: **0.7688**

For automated future retraining, the Docker image only needs to be built once:

```bash
docker compose --profile training build ml-trainer
```

Airflow can then run the one-shot trainer through Docker only when its upstream ML export or trainer image version changes.

---

## Streamlit

Main entry point:

```text
streamlit/app.py
```

Pages:

- **Overview** — total trips, revenue, average duration/distance, daily trends
- **Taxi Zone Map** — interactive choropleth by trip count, revenue, duration, distance, or tip
- **Demand Patterns** — day/hour heatmap + hourly pickup-demand curve
- **Trip Duration Predictor** — direct inference from the deployed `.pkl` model

`spark/export_dashboard_data.py` exports the compact Gold serving tables to `streamlit/data/`.

The deployed Streamlit app does **not** run Hadoop, Kafka, Spark, or Airflow for each visitor. Those systems create the serving artifacts; Streamlit only reads the compact outputs.

The Airflow DAG can refresh the local Docker Streamlit container when dashboard/model artifacts are rebuilt. Streamlit Community Cloud deployment remains a separate GitHub deployment concern; the DAG does not automatically commit or push generated artifacts to GitHub.

---

## Repository Structure

```text
NYC/
├── airflow/
│   └── dags/
│       ├── nyc_taxi_pipeline.py
│       └── nyc_taxi_streaming_pipeline.py
├── ingestion/
│   ├── hdfs_batch_loader.py
│   ├── kafka_streaming_producer.py
│   ├── Dockerfile
│   └── requirements.txt
├── spark/
│   ├── spark_streaming_consumer.py
│   ├── bronze_to_silver.py
│   ├── build_gold.py
│   ├── validate_final_silver.py
│   ├── validate_final_gold.py
│   ├── build_duration_ml_dataset.py
│   ├── train_duration_models.py
│   ├── export_ml_dataset_for_deployment.py
│   ├── export_dashboard_data.py
│   └── predict_trip_duration.py
├── ml_training/
│   ├── train_deployable_model.py
│   ├── Dockerfile
│   └── requirements.txt
├── ml_artifacts/
│   ├── trip_duration_model.pkl
│   ├── trip_duration_model_metrics.json
│   └── trip_duration_model_features.json
├── streamlit/
│   ├── app.py
│   ├── utils.py
│   ├── pages/
│   ├── data/
│   └── assets/taxi_zones/
├── docker-compose.yml
├── hadoop.env
├── .env.example
└── README.md
```

---

## Running the Project

Create `.env` from `.env.example` and point `NYC_DATA_PATH` to the local NYC data directory. The final streaming month is:

```env
STREAMING_MONTH=2025-12
```

Start the infrastructure:

```bash
docker compose up -d --build
```

Build the optional trainer image once so Airflow can retrain the portable model if it ever becomes necessary:

```bash
docker compose --profile training build ml-trainer
```

Useful UIs:

- HDFS NameNode: `http://localhost:9870`
- Spark Master: `http://localhost:8080`
- Spark Worker: `http://localhost:8081`
- Airflow: `http://localhost:8082`
- Streamlit: `http://localhost:8501`

Run the streaming DAG first if December 2025 streaming Bronze has not already been completed:

```text
nyc_taxi_streaming_ingestion
```

Then run the full orchestration DAG:

```text
nyc_taxi_full_pipeline
```

On an already-completed project, the first run should primarily **bootstrap fingerprints and reuse the existing outputs**, not rebuild the entire pipeline.
