# NYC Taxi Big Data - Docker Setup

This Docker stack contains:

- HDFS NameNode + DataNode
- Apache Kafka in KRaft mode (no ZooKeeper)
- Spark standalone Master + Worker
- Python ingestion container for the batch loader and Kafka producer

Your local NYC dataset is mounted read-only at `/data` inside the ingestion container.

## 1. Local data structure

Keep your Windows data like this:

```text
NYC Data/
├── trip_records/
│   ├── yellow_tripdata_....parquet
│   └── ...
└── taxi_zones/
    ├── taxi_zone_lookup.csv
    ├── taxi_zones.shp
    ├── taxi_zones.dbf
    ├── taxi_zones.shx
    ├── taxi_zones.prj
    └── taxi_zones.cpg
```

## 2. Configure `.env`

Copy `.env.example` to `.env` and replace the Windows username/path.
Use forward slashes, for example:

```env
NYC_DATA_PATH=C:/Users/Aly/Downloads/NYC Data
STREAMING_MONTH=2025-01
```

The streaming month must correspond to a file such as:

```text
yellow_tripdata_2025-01.parquet
```

## 3. Start the stack

From this project directory:

```bash
docker compose up -d --build
```

Then check:

```bash
docker compose ps
```

Useful browser UIs:

- HDFS NameNode: http://localhost:9870
- Spark Master: http://localhost:8080
- Spark Worker: http://localhost:8081

Kafka is reachable from the host at `localhost:9094` and from containers at `kafka:9092`.

## 4. Verify the mounted local data

```bash
docker compose exec ingestion sh -lc 'ls -lah /data && echo && ls -lah /data/trip_records | head && echo && ls -lah /data/taxi_zones'
```

## 5. Run batch + reference ingestion

This sends all monthly Parquet files to HDFS Bronze EXCEPT `STREAMING_MONTH`.
It also uploads the taxi-zone reference files once.

```bash
docker compose exec ingestion sh -lc 'python /opt/project/hdfs_batch_loader.py --source-dir /data/trip_records --streaming-month "$STREAMING_MONTH" --reference-dir /data/taxi_zones'
```

## 6. Check HDFS

```bash
docker compose exec namenode hdfs dfs -ls -R /nyc-taxi
```

Expected top-level areas include:

```text
/nyc-taxi/bronze/batch
/nyc-taxi/reference
/nyc-taxi/control
```

## 7. Start the Spark Structured Streaming consumer

Run this in one terminal and leave it open:

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.driver.host=spark-master \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 \
  /opt/project/spark_streaming_consumer.py \
  --streaming-month 2025-01
```

Replace `2025-01` with the same value used in `.env`.

The first run downloads the Spark Kafka connector from Maven, so it may take a little longer.

## 8. Run the Kafka producer

In a second terminal:

```bash
docker compose exec ingestion sh -lc 'python /opt/project/kafka_streaming_producer.py --file "/data/trip_records/yellow_tripdata_${STREAMING_MONTH}.parquet" --month "$STREAMING_MONTH" --delay 0.01'
```

For a quick test first, add:

```text
--max-rows 10000
```

## 9. Check streaming Bronze output

```bash
docker compose exec namenode hdfs dfs -ls -R /nyc-taxi/bronze/streaming
```

When the producer is finished and Spark has processed its last micro-batch, stop the consumer with `Ctrl+C`.

## 10. Stop Docker safely

```bash
docker compose down
```

The HDFS NameNode/DataNode data remains in Docker named volumes.

Do not use `docker compose down -v` unless you deliberately want to delete/reset all HDFS data.
