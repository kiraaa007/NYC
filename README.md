# NYC Yellow Taxi Big Data Analytics

A hybrid **batch + streaming Big Data project** built on five years of NYC TLC Yellow Taxi data, covering **January 2021 through December 2025 (60 months)**.

The project combines Hadoop HDFS, Apache Kafka, Apache Spark, Spark Structured Streaming, Apache Airflow, machine learning, Docker, and Streamlit to build an end-to-end data platform from raw taxi trip files to analytics, geospatial visualization, and trip-duration prediction.

## Live Project

- **Streamlit App:** https://ax8afezqcq6m6mbezghqhj.streamlit.app/
- **GitHub Repository:** https://github.com/kiraaa007/NYC

---

## 1. Project Architecture

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
        +-------+--------+
        |                |
        v                v
   Spark Gold         ML Dataset
   Aggregations       + Feature Engineering
        |                |
        v                v
 Dashboard Export    Model Training
        |                |
        |          Portable .pkl Model
        +-------+--------+
                |
                v
             Streamlit
    Analytics + Map + ML Predictor
```

The project intentionally keeps the large raw and processed trip data in HDFS. Streamlit consumes only compact Gold-layer exports, the taxi-zone reference files, and the portable ML model.

---

## 2. Dataset Scope

The project uses NYC TLC **Yellow Taxi** monthly Parquet files for:

```text
2021-01 through 2025-12
60 total months
```

The final ingestion split is:

```text
Batch ingestion     : 59 months
Streaming ingestion : 2025-12 only
```

December 2025 is deliberately excluded from the batch loader and is instead replayed row-by-row through Kafka to simulate a finite streaming workload.

Static reference data includes:

```text
taxi_zone_lookup.csv
taxi_zones.shp
taxi_zones.dbf
taxi_zones.shx
taxi_zones.prj
taxi_zones.cpg
```

The taxi-zone files are loaded as reference data and are never sent through Kafka.

---

## 3. Technology Stack

| Layer | Technology | Purpose |
|---|---|---|
| Containerization | Docker Compose | Runs the project services in one reproducible environment |
| Distributed storage | Hadoop HDFS | Stores Bronze, Silver, Gold, checkpoints, controls, and references |
| Batch ingestion | Python + WebHDFS | Loads historical monthly Parquet files into HDFS |
| Streaming ingestion | Apache Kafka | Simulates one live month of taxi trips |
| Streaming processing | Spark Structured Streaming | Consumes Kafka and writes streaming Bronze data to HDFS |
| Batch processing | Apache Spark | Cleans, normalizes, joins, aggregates, and prepares ML data |
| Orchestration | Apache Airflow | Coordinates ingestion and Bronze -> Silver -> Gold workflows |
| ML | Spark ML + scikit-learn | Model comparison and deployable trip-duration model |
| Visualization | Streamlit + Plotly + GeoPandas | Dashboard, demand patterns, map, and ML inference |
| Deployment | Streamlit Community Cloud | Hosts the final interactive application |

---

## 4. Docker Services

`docker-compose.yml` defines the full local environment:

| Service | Role |
|---|---|
| `namenode` | HDFS NameNode |
| `datanode` | HDFS DataNode |
| `kafka` | Kafka broker/controller in KRaft mode |
| `kafka-init` | Creates topic `nyc_taxi_stream` |
| `spark-master` | Spark standalone master |
| `spark-worker` | Spark worker |
| `ingestion` | Runs the batch loader and Kafka producer |
| `airflow-postgres` | Airflow metadata database |
| `airflow-init` | Initializes the Airflow database and admin user |
| `airflow-webserver` | Airflow UI |
| `airflow-scheduler` | Executes Airflow DAGs and Docker tasks |
| `ml-trainer` | Trains the portable deployment model |
| `streamlit-dashboard` | Runs the local Streamlit application |

HDFS uses persistent Docker volumes. Kafka is used as a transient streaming transport; HDFS and Spark checkpoints provide the durable processing state.

---

## 5. Ingestion Layer

### `ingestion/hdfs_batch_loader.py`

Loads historical Yellow Taxi Parquet files to HDFS Bronze while excluding the configured streaming month.

Main behavior:

```text
Historical Parquet -> HDFS /nyc-taxi/bronze/batch/year=YYYY/month=MM
```

It also uploads the taxi-zone reference files to:

```text
/nyc-taxi/reference
```

The loader is idempotent. After a month is successfully loaded it writes a marker under:

```text
/nyc-taxi/control/batch_loaded/YYYY-MM/_SUCCESS
```

Existing successful months are skipped on later runs.

### `ingestion/kafka_streaming_producer.py`

Simulates streaming for exactly one monthly Parquet file: **2025-12**.

Each taxi trip is serialized as JSON and sent as an individual Kafka message to:

```text
nyc_taxi_stream
```

The file is read incrementally with PyArrow batches so millions of rows are not loaded into memory at once.

At the end, the producer sends one control record:

```json
{"__eof__": true, "year_month": "2025-12"}
```

---

## 6. Streaming Layer

### `spark/spark_streaming_consumer.py`

Consumes `nyc_taxi_stream` using Spark Structured Streaming.

It performs ingestion only:

```text
Kafka -> Spark Structured Streaming -> HDFS Bronze Streaming
```

Output:

```text
/nyc-taxi/bronze/streaming/year=2025/month=12
```

The consumer keeps the taxi payload raw as JSON and preserves Kafka metadata:

```text
kafka_topic
kafka_partition
kafka_offset
kafka_timestamp
```

The EOF message is filtered out.

A Spark checkpoint is maintained at:

```text
/nyc-taxi/checkpoints/streaming_ingestion
```

This allows Spark to resume from committed Kafka offsets rather than replaying already processed records.

---

## 7. Airflow Orchestration

The project contains two Airflow DAGs.

### `airflow/dags/nyc_taxi_streaming_pipeline.py`

DAG ID:

```text
nyc_taxi_streaming_ingestion
```

Flow:

```text
Check services
-> Ensure Kafka topic
-> Inspect current streaming state
-> Start Spark streaming consumer when required
-> Run Kafka producer
-> Wait for Kafka/Spark completion
-> Stop consumer
-> Write HDFS success marker
```

The DAG is idempotent and contains recovery logic for a Kafka broker reset. If HDFS Bronze plus Spark checkpoints prove the complete stream was already committed, it blocks producer replay to prevent duplicate December data.

### `airflow/dags/nyc_taxi_pipeline.py`

DAG ID:

```text
nyc_taxi_batch_to_gold
```

Flow:

```text
Check services
-> Batch + reference ingestion
-> Verify Batch Bronze
-> Verify Streaming Bronze
-> Detect whether Bronze/reference state changed
-> Build Silver only if required
-> Validate Silver
-> Build Gold
-> Validate Gold
-> Record pipeline fingerprint
```

A SHA-256 fingerprint is generated from durable Bronze/reference state. If the fingerprint has not changed, Airflow skips the expensive Silver and Gold rebuild.

---

## 8. Bronze -> Silver Processing

### `spark/bronze_to_silver.py`

This is the main transformation job.

Batch Bronze contains normal Parquet columns, while Streaming Bronze contains raw JSON. The job converts both sources into one canonical schema and unions them.

It also handles schema drift across TLC years. Columns missing from a particular month are safely represented as null rather than breaking the full multi-year read.

Silver cleaning is intentionally conservative. Structurally unusable records are removed, including missing timestamps, invalid pickup month, dropoff before pickup, missing pickup/dropoff location IDs, negative distance, and negative passenger count.

The job derives:

```text
trip_duration_minutes
pickup_date
pickup_year
pickup_month
pickup_day
pickup_hour
pickup_day_of_week
```

It then joins `taxi_zone_lookup.csv` twice to enrich both pickup and dropoff IDs with:

```text
Zone
Borough
service_zone
```

Output:

```text
/nyc-taxi/silver/trips
```

partitioned by:

```text
pickup_year
pickup_month
```

Final Silver contains **198,091,763 trips** across **60 year-month partitions**.

---

## 9. Silver -> Gold Analytics

### `spark/build_gold.py`

Builds compact serving tables for analytics and Streamlit.

Output tables:

| Gold Table | Purpose |
|---|---|
| `daily_summary` | Daily trips, revenue, distance, duration, tips, passenger metrics |
| `hourly_demand` | Demand by hour and day of week |
| `pickup_zone_monthly` | Monthly pickup-zone performance and geographic analytics |
| `payment_monthly` | Monthly metrics by payment type |

All final Gold tables reconcile back to **198,091,763 Silver trips** and cover all **60 months**.

### Validation files

- `spark/validate_silver_tests.py` - development validation for sample batch/streaming Silver outputs
- `spark/validate_final_silver.py` - final Silver row, partition, source, and zone-enrichment checks
- `spark/validate_gold_test.py` - development Gold inspection
- `spark/validate_final_gold.py` - final 60-month coverage and trip-count reconciliation

---

## 10. Dashboard Export

### `spark/export_dashboard_data.py`

Streamlit does not read the full Silver dataset.

This job exports the compact Gold tables from HDFS to:

```text
streamlit/data/
```

The deployed dashboard therefore reads only small pre-aggregated Parquet serving tables instead of scanning the full Big Data dataset.

---

## 11. Machine Learning

The ML target is:

```text
trip_duration_minutes
```

### `spark/build_duration_ml_dataset.py`

Creates prediction-time features from Silver and uses a temporal train/test split:

```text
Train : 2021-01 through 2025-11
Test  : 2025-12
```

ML-specific quality filters constrain duration, distance, passenger count, location IDs, rate codes, and time fields.

Features include pickup/dropoff location, passenger count, distance, rate code, pickup hour/month/day, weekend/rush-hour indicators, and cyclical hour/month features.

### `spark/train_duration_models.py`

Compares distributed Spark ML regressors:

```text
Mean baseline
Linear Regression
Random Forest Regressor
Gradient-Boosted Trees Regressor
```

Metrics:

```text
MAE
RMSE
R²
```

The best trained Spark model is selected by RMSE and persisted to HDFS.

### `spark/predict_trip_duration.py`

Provides command-line inference for the persisted Spark GBT pipeline and verifies that the saved Spark model can be reloaded successfully.

---

## 12. Portable Deployment Model

The deployed Streamlit application does not start Spark for every prediction.

### `spark/export_ml_dataset_for_deployment.py`

Exports the sampled Spark ML train/test datasets from HDFS to local `ml_artifacts/data/` for portable model training.

### `ml_training/train_deployable_model.py`

Trains a scikit-learn `HistGradientBoostingRegressor` pipeline with target encoding for categorical IDs and saves it using Joblib.

Artifacts:

```text
ml_artifacts/trip_duration_model.pkl
ml_artifacts/trip_duration_model_metrics.json
ml_artifacts/trip_duration_model_features.json
```

Current deployed model evaluation on unseen **December 2025** data:

| Metric | Result |
|---|---:|
| Training rows | 841,051 |
| Test rows | 143,109 |
| MAE | 4.415 min |
| RMSE | 6.996 min |
| R² | 0.7688 |

The `.pkl` model is loaded directly by Streamlit for lightweight inference.

---

## 13. Streamlit Application

Entry point:

```text
streamlit/app.py
```

### Overview

Displays:

```text
Trips
Revenue
Average trip duration
Average trip distance
Daily trip volume
Daily revenue
```

### `streamlit/pages/1_Zone_Map.py`

Interactive NYC taxi-zone choropleth using the TLC shapefile and Gold `pickup_zone_monthly` table.

Users can select year, month, and map metric such as trip count, revenue, average duration, average distance, or average tip.

### `streamlit/pages/2_Demand_Patterns.py`

Displays:

```text
Day-of-week x hour demand heatmap
Hourly pickup-demand line chart
```

### `streamlit/pages/3_Trip_Duration_Predictor.py`

Loads `trip_duration_model.pkl` directly and predicts the planned taxi-trip duration from:

```text
Pickup zone
Dropoff zone
Passenger count
Estimated trip distance
Pickup date/time
Rate code
```

The page also displays the saved test MAE, RMSE, and R² metrics.

### `streamlit/utils.py`

Shared loader and caching functions for Gold tables, taxi-zone lookup data, and the taxi-zone shapefile. It supports both Docker paths and repository-relative Streamlit Community Cloud paths.

---

## 14. Repository Structure

```text
NYC/
├── airflow/
│   └── dags/
│       ├── nyc_taxi_pipeline.py
│       └── nyc_taxi_streaming_pipeline.py
├── ingestion/
│   ├── Dockerfile
│   ├── hdfs_batch_loader.py
│   ├── kafka_streaming_producer.py
│   └── requirements.txt
├── spark/
│   ├── spark_streaming_consumer.py
│   ├── bronze_to_silver.py
│   ├── build_gold.py
│   ├── build_duration_ml_dataset.py
│   ├── train_duration_models.py
│   ├── predict_trip_duration.py
│   ├── export_dashboard_data.py
│   ├── export_ml_dataset_for_deployment.py
│   └── validation scripts
├── ml_training/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── train_deployable_model.py
├── ml_artifacts/
│   ├── trip_duration_model.pkl
│   ├── trip_duration_model_metrics.json
│   └── trip_duration_model_features.json
├── streamlit/
│   ├── app.py
│   ├── utils.py
│   ├── requirements.txt
│   ├── data/
│   ├── assets/taxi_zones/
│   └── pages/
├── docker-compose.yml
├── hadoop.env
├── .env.example
└── README.md
```

---

## 15. Local Setup

Create `.env` from `.env.example` and point it to the local NYC dataset:

```env
NYC_DATA_PATH=C:/path/to/NYC Data
STREAMING_MONTH=2025-12
```

Expected local data layout:

```text
NYC Data/
├── trip_records/
│   ├── yellow_tripdata_2021-01.parquet
│   ├── ...
│   └── yellow_tripdata_2025-12.parquet
└── taxi_zones/
    ├── taxi_zone_lookup.csv
    ├── taxi_zones.shp
    ├── taxi_zones.dbf
    ├── taxi_zones.shx
    ├── taxi_zones.prj
    └── taxi_zones.cpg
```

Start the environment:

```bash
docker compose up -d --build
```

Useful local UIs:

```text
HDFS NameNode : http://localhost:9870
Spark Master  : http://localhost:8080
Spark Worker  : http://localhost:8081
Airflow       : http://localhost:8082
Streamlit     : http://localhost:8501
```

Do not run `docker compose down -v` unless the HDFS Docker volumes are intentionally being deleted.

---

## 16. Final Pipeline Summary

```text
5 years / 60 months of NYC Yellow Taxi data
                    |
          59 Batch + 1 Streaming
                    |
          HDFS Bronze storage
                    |
       Spark normalization + cleaning
                    |
              HDFS Silver
          198,091,763 trips
                    |
        +-----------+-----------+
        |                       |
   Gold analytics            ML pipeline
        |                       |
        v                       v
 Streamlit visualizations   .pkl predictor
        +-----------+-----------+
                    |
                    v
          Deployed Streamlit App
```

This repository represents the final project implementation. The current source code, Airflow DAGs, final validation scripts, ML artifacts, and Streamlit application are the source of truth for the project architecture and behavior.
