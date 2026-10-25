# Zone view

This note provides detail about jobs. Since this directory is the place where everything starts, it's suitable to put the `README.md` here. The structure follows Lambda architecture (batch, speed, serving layer)

![Data flow](../flow/data_flow.svg)

## Pre-require knowledge for the architecture

- [Medallion architecture](https://dev.to/datanestdigital/medallion-architecture-in-databricks-a-complete-implementation-guide-2lb9)
- [Real-time processing](https://dev.to/datanestdigital/real-time-data-streaming-with-apache-kafka-and-spark-c4l)

## Start the infrastructure

- Ref for **building infra and preparing env variables**
    - [Airflow](https://github.com/apache/airflow/blob/main/airflow-core/docs/howto/docker-compose/docker-compose.yaml)
    - [Confluent](https://github.com/confluentinc/cp-all-in-one/blob/8.2.0-post/cp-all-in-one/docker-compose.yml)
    - Read `./.env_file/*.env.example` to create `./.env_file/*.env`

- For the first time, it is required to **add Debezium (as a plugin)** into `connect` by customizing the `connect` image (see `./cdc/Dockerfile`). This custom image is used as `connect` in the compose file

    ```bash
    cd ./cdc
    docker built -t local/cp-server-connect-debezium:0.6.4-7.6.0 .
    ```

    - `make up-i1` also builds the `connect`. See `../Makefile`

- **Config catalog** for `trino` as in `./trino/catalog/lakehouse.properties`. This config is mounted to trino in the compose file

- There are 2 `*-docker-compose.yml` files to be started. One runs `airflow`, the other creates infra for batch and speed layer. The `./.env_file/*.env` files have been referred in those compose files

- Read `Makefile` then run the following commands

    ```bash
    make up-a & make up-i
    ```

- Once `airflow` has healthily started, import environment variables (`./.env_file/*.json`, refer to `./.env_file/*.json.example`) into it so its worker can work securely with the variables (Admin >> Variables >> Import Variables)

- **Build environment for jobs**
    - Refer to `./airflow/dags/docker_env/Dockerfile` to build an env image
    - This image is used as an environment to run all jobs in this work

## Environment for running jobs

- Ref `./dags/docker_env/Dockerfile`
- Every job is packaged in a Docker environment, which ensures that they produces the same output as in dev environment
- The environment includes:
    - Python 3.11
    - Python packages defined in `./dags/docker_env/requirements.in`
    - Java21, utilities (nano, curl)
    - Spark's jar dependencies
        - Delta Lake: `io.delta:delta-spark_4.1_2.13:4.3.0`
        - Trino: `io.trino:trino-jdbc:480`
        - AWS: `org.apache.hadoop:hadoop-aws:3.4.2, software.amazon.awssdk:bundle:2.29.52`
        - Postgres: `org.postgresql:postgresql:42.7.11`
        - Kafka: `org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1`
        - Avro: `org.apache.spark:spark-avro_2.13:4.1.1`
- Build: `docker build -t longnb32/spark_env:<version>`
- Output: After building the environment, we obtain a docker image which can be used as an environment to run Python code

## Batch watermark

- The watermarks are maintained at `../tracking`. They are required to run jobs
    - `track_bronze`: Contain a list of `source_created_datetime`. Each element (at a time) is the newest processed value in Bronze zone. Initialized value: `2000-01-01 00:00:00.0`
    - `track_silver`: Same logic to `track_bronze`, but the list contains the newest `bronze_created_datetime` from Silver zone
    - `track_gold`: Same logic to `track_bronze`, but the list contains the newest `silver_created_datetime` from Gold zone
    - `track_produce_data`: The only file doesn't follow watermark logic above. It contains a list of unprocessed parquet file. Once the file is processed, its name will be removed from the list

- Usage: For example, a job needs to read from datasource and pour raw data into Bronze zone
    - At a time, it can only handle an amount of data
    - After handling that amount of data, it records the latest `source_created_datetime` of that amount (aka `watermark`)
    - Next time, it will process (from datasource) all records whose `source_created_datetime` is greater than `watermark` and record the new `watermark`

## Datasource

- Ref: `./dags/docker_env/dag_produce_data.py`

- Parquet files: There is a parquet file generated every month which includes all trip records in that month. Format: `yellow_tripdata_YYYY_MM.parquet`. Since I handles those files from 2009 to 2025, there are 204 parquet files

- Related tables: A Python script reads these file and insert data into 2 Postgres tables to simulate the data scenario:
    - `nyc_taxi`: Contain the whole data processed by batch jobs
    - `nyc_taxi_stream`: Contain a portion of data handled by stream jobs
        - Debezium is setup to read changelogs from this table and publish messages to a Kafka topic (see `../cdc/configs/postgres-cdc.json`)
    - Purpose: First, a portion of data is inserted into `nyc_taxi_streams` to simulate the CDC source (Debezium reads changelog and writes to a Kafka topic) for stream jobs. Then the whole file is inserted into `nyc_taxi` for batch jobs to correct stream's output

- Process
    - It requires `wal_level=logical` in Postgres so the changelog can be captured. See `../infra-docker-compose.yml`
    - The job processes the first filename in `../tracking/track_produce_data` every minute. After that, the filename is removed from the tracking file.
    - There are 3 modes to trigger (modified in the dag file)
        - `batch`: Push the whole file into `nyc_taxi` and update tracking file
        - `stream`: Split 20% of the file into chunks and insert into `nyc_taxi_stream`. `stream` mode doesn't update tracking file
        - `both`: Perform both `batch` and `stream`. Then update the tracking file

## Bronze zone

- Ref
    - `./dags/docker_env/dag_process_data.py`: Batch job
    - `./dags/docker_env/dag_bronze_ingest.py`: Stream job

- Related tables: Source table in Datasource (database: `company_db`) and delta table in LakeHouse
    - `lakehouse.bronze.nyc_taxi`: Spark reads `nyc_taxi` in datasource and inserts raw data into this table
    - `lakehouse.bronze.nyc_taxi_stream`: Spark streaming reads Kafka topic (from CDC source) and inserts raw data into this table

- Process
    - Batch job:
        - Read datasource from watermark specified in `../tracking/track_bronze`
        - The job adds a `record_key` to every record (hash all data)
        - Add audit field: `bronze_created_datetime`
        - Write to a delta table, partition by month and year
        - Update watermark for batch job: Insert the new one into `../tracking/track_bronze`
    - Stream job does the same logic but the data source is a Kafka topic, which requires to decode the message based on specified format (`avro, json`) and schema. Since Debezium registers the message's schema to `schema-registry`. Just take the schema from `schema-registry` to decode the message

- Trino can be used to query data from Bronze zone (use DBCode or just go straight into the container and query in there)

## Silver zone

- Ref
    - `./dags/docker_env/dag_process_data.py`: Batch job
    - `./dags/docker_env/dag_bronze_ingest.py`: Stream job

- Related tables: Delta tables in LakeHouse
    - `lakehouse.silver.nyc_taxi`: Spark reads `lakehouse.bronze.nyc_taxi`, filters by watermark, processes the data and upsert into `lakehouse.silver.nyc_taxi`
    - `lakehouse.silver.nyc_taxi_stream`: Spark streaming reads `lakehouse.bronze.nyc_taxi`, performs the same processes and upsert into `lakehouse.silver.nyc_taxi_stream`

- Process
    - Batch job
        - Reads Bronze zone from the watermark in `../tracking/track_silver`. Meanwhile stream job readStreams from Bronze zone
        - Enrich data by adding business key corresponding to fields, hash the content to perform data-deduplication
        - Add audit fields: `silver_created_datetime`, `silver_updated_datetime` (since we upsert into silver tables)
        - Upsert into Silver zone
        - Update watermark for batch job: Insert the new one into `../tracking/track_silver`

    - Stream job does the same logic but the source data is `readStream` from Bronze zone (`lakehouse.bronze.nyc_taxi_stream`). CDF is not required to turn on since the previous job only appends to Bronze zone (**if there's any update/delete without CDF turned on, the code will crash**)

- Trino can be used to query data from Silver zone (use DBCode or just go straight into the container and query in there)

## Gold zone

- Ref
    - `./dags/docker_env/dag_gold_setup.py`: Setup Gold zone, run in batch mode
    - `./dags/docker_env/dag_gold_modeling.py`: Batch job
    - `./dags/docker_env/dag_gold_stream.py`: Stream job

- Related tables: Tables in Postgres (database: `gold`)
    - `staging.dim_{payment_type, vendor, rate_code}`: Temporary table, upsert new data from Silver (Delta table) to Gold (Postgres table). The purpose is to maintain SCD type 2 on dim tables using `dbt snapshot` (which requires to read a table with updated records)
    - `staging.nyc_taxi`: Temporary table, overwrite new data from Silver to perform upsert (into `fact_trips`) by `dbt run`
    - `public.fact_trips_stream_tmp`: Temporary table, join stream data to dim tables from batch layer (with cache, since speed is required, there's no time to create dim tables in speed layer) and overwrite to this table. Then upsert the table to `fact_trips_stream`

    - `public.dim_{date, location, store_fwd}`: Dim tables created by `dbt seed`
    - `public.dim_{payment_type, vendor, rate_code}`: Dim tables (SCD type 2) obtained by performing `dbt snapshot` on `staging.dim_{payment_type, vendor, rate_code}`
    - `public.fact_trips`: Fact table of batch layer
    - `public.fact_trips_stream`: Fact table of stream layer
    - All dim tables are maintained by batch layer

- Process
    ![Gold logic](../flow/gold_logic.svg)

    - Setup gold zone (dag `gold.setup_gold`)
        - Create gold tables
            - `public.dim_{date, location, store_fwd}`: Ensure Spark writes the right datatype to these table
            - `staging.dim_{payment_type, vendor, rate_code}`: Create tables for Spark to upsert into
            - `public.fact_trips_stream`: This table is upsert into by SparkStreaming, so it must be created in advanced

        - Generate data for `dim_date` with a pre-defined range. If the data in the table is not in the range, then re-generate and overwrite

    - Seed `dim_{location, store_fwd}` from `*.csv` files (since these data is rarely changed) using `dbt seed` and resolve dependency using `dbt deps` (dag `deps_seed`)

    - Batch job
        - Extract (via watermark), then:
            - Upsert into dim tables in `staging` using Spark
            - Overwrite to a temporary table (`staging.nyc_taxi`)
        - `dim snapshot` to maintain SCD type 2 for dim tables
        - Join `staging.nyc_taxi` with dim tables to model fact table, then upsert into `fact_trips`

    - Stream job only aims to maintain the fact tables
        - It `readStream` table `lakehouse.silver.nyc_taxi_stream` with CDF turned on to capture all events (`_change_type`) of a record (`insert`, `preimage`, `postimage`, `delete`)
        - Filter only `insert` and `postimage` (after-change version of a record) and join to dim tables (from batch layer) to model fact table
        - Upsert that dataframe into `public.fact_trips_stream`

    - Serving layer: A view to combine `public.fact_trips` and `public.fact_trips_stream`, which take all the historical data from batch layer and the newest one (based on watermark, year, and month) of stream layer

## Orchestration

![Data flow](../flow/techstack.svg)

- Access <http://localhost:8080>

- Produce data: Run every 1 minute, push data into batch and stream table (for simulation purpose. In practice, just capture changelog directly from source table)

- Batch layer
    - Dag `Bronze >> Silver >> Gold`. Run every 10 minutes and take 5 minutes to run. Then, in the meanwhile, stream jobs take the place to ensure that the serving view has the newest data

    - Based on the idea of broker (Kafka), it's totally possible to design 3 separated, independent jobs which can run in parallel. Every job takes care of their own task (with rigorous watermark strategy to make sure that there's no new record in the time that a dataframe is processed). But there're some errors that I can't debug appearing randomly in dags. Thus, I designed a fixed pipeline `Bronze >> Silver >> Gold`

- Speed layer
    - The idea is that I have a console to manage the dag and the environment should be isolated. Thus, I wrapped the stream code into a Docker environment
    - 3 jobs behave exactly in stream mode (`awaitTermination`)

- Serving layer: This layer is not included in the orchestration but calculated at the time it's accessed by end user
