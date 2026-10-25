
# ml-ops

- A full-stack MLOps project which aims at 3 points
    - Data pipeline: Build a data pipeline
    - AI: Automatically train model
    - Ops: Build infrastructure for running data pipeline and training model

- Todo
    - More DQ (e.g., Great Expectation)
    - AI/Data visualization for this data

## Data pipeline

### A brief on Problem, Architecture, Tech-stack

- Data description: [Taxi trip records in New York City](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) is a dataset capturing all taxi trips (pickup/drop-off datetime or location, types of fee, number of passengers,...) in New York City from 2009. **The dataset contains about 1.5 billion records (~170GB).**

- **Problem**: This work aims to **implement a full Lambda pipeline that simulates the datasource, processes data through zones, and serves clean data to end user** (Data Analyst, Data Scientist)

- **Architecture**: **Lambda architecture** is adopt as follows

    ![Data flow](./flow//data_flow.svg)

    - In which, batch layer and speed layer are separated and possesses their own storage (i.e., separated batch and streaming table for each zone). Serving layer is constructed based on the combination of batch layer and speed layer.

    - Data source is simulated for batch job and streaming job. Batch job processes/transforms data based on watermark. Streaming job applies same transform logic on data obtained by CDC (Debezium or CDF). Serving layer is union from historical batch data and newest data from speed layer.

    - Medallion architecture in this work
        - Bronze: **Append-only, contain raw data, partition by month**, ready to re-run
        - Silver: Data from bronze is cleaned, transformed, deduplicated, and **upsert into Silver**
        - Gold: **Star schema is adopt**. Data from Silver is modeled and upsert into Gold zone. Maintain **SCD type 2 for dim tables**

    - Source code is packaged into modules as demonstrated in the image

- **Tech-stack** in this work is demonstrated in the following image
    ![Tech-stack](./flow/techstack.svg)

    - Datasource
        - `Postgres` is chosen to be the datasource as it natively supports **logical replication**, suited for simulating streaming source. There're also **many connector to Postgres** (Trino, Debezium, SinkJDBC)
        - CDC (`Debezium`) is used to simulated streaming data, and works well with `Kafka`

    - Batch layer focus on adopting Lakehouse architecture which supports **ACID, partition, schema evolution, schema enforcement, and data versioning**
        ![Lakehouse architecture](./flow/lakehouse.svg)

        - Lakehouse:
            - `Trino` as a query engine
            - `Hive` as a metadata store
            - `Delta lake` as the table format
            - `Postgres` as a meta-database
            - AWS S3 as the object storage

        - Processing
            - `PySpark` as main processing technique
            - `dbt` for showcase
            - `Airflow` as orchestrator

    - Speed layer: Custom [Confluent stack](https://github.com/confluentinc/cp-all-in-one/blob/8.2.0-post/cp-all-in-one/docker-compose.yml) so the system is more lightweight (drop monitoring and other stuff)
        - Infra
            - `Debezium` for capturing change log from Data source and pour into a Kafka topic
            - `Kafka` as a **broker**
            - `schema-registry` for storing message's schema
            - `control-center` as a console for connector, Kafka topic, metric monitoring

        - Processing: `SparkStreaming` as main processing technique

    - **Serving layer uses Postgres with Star schema**. Both layers have their own serving tables. The batch one maintains a schema model. The speed one has its own fact table (joined coming data to dim tables from batch).

### Project structure

```md
.
├── .env_file                   # Environment variables
├── airflow                     # Contain all DAGs (produce data, batch, speed, optimize)
│   ├── config
│   ├── dags
│   |   └── docker_env          # All jobs are run in a docker environment
|   └── README.md               # **Explain detail about most of everything in the project**
├── cdc                         # Build `connect` with plugin `Debezium`
│   └── configs
├── flow                        # Design of pipeline
├── spark                       # Checkpoint of spark
│   └── checkpoint
│       ├── bronze_stream
│       ├── gold_stream
│       └── silver_stream
├── sql                         # Initialize of some tables and **serving view**
├── src
│   ├── bronze                  # Ingest into and validate Bronze zone
│   ├── gold                    # Modeling schema for Gold zone
│   │   └── dbt_gold            # Modeling using `dbt`
│   │       ├── analyses
│   │       ├── models
│   │       │   ├── dimensions
│   │       │   └── fact
│   │       ├── seeds
│   │       └── snapshots
│   ├── notebooks               # Draft before code
│   │   ├── test_infra
│   │   └── test_process_data
│   │       ├── ingest_bronze
│   │       ├── ingest_gold
│   │       ├── ingest_silver
│   │       └── produce_data
│   ├── silver                  # Clean, transform, enrich data before merge into Silver zone
│   └── utils                   # Produce data and other utils stuff
├── tracking                    # Watermark for 3 zones
├── trino                       # Config catalog for `trino`
|   ├── catalog
|   └── etc
├── requirements.in             # Dependencies
├── airflow-docker-compose.yml  # Start Airflow
├── infra-docker-compose.yml    # Start Infrastructure
└── Makefile                    # Shortcut for commands
```

### Quickstart

- This instruction shows the way to run the system without knowing detail about it. In order to capture the logic of code, see `./airflow/README.md`

- Infrastructure
    - Prepare `./.env_file/*.env` files as examples at the same directory
    - Start Airflow and Infrastructure. **For the first time**, use `make up-i1` instead of `make up-i`

        ```bash
        make up-a & make up-i
        ```

    - Check health for all service (`docker ps -a | grep -E "ctn|air"`)
    - Register connector to read changelog from Datasource (Postgres table)

        ```bash
        cd ./cdc
        sh connector_register.sh ./configs/postgres-cdc.json
        ```

        - Check the status at `control-center` (<http://localhost:9021>), tab `Connector`

- Source
    - Prepare `*.parquet` files on AWS S3
    - Access Airflow UI (<http://localhost:8080>)
    - Active DAG `Produce Data`

- Batch layer
    - Access Airflow UI (<http://localhost:8080>)
    - Active DAG `Bronze >> Silver >> Gold`
    - Batch job should be run at least once before starting Stream job since Stream job depends on some material generated by Batch job

- Speed layer
    - Access Airflow UI (<http://localhost:8080>)
    - Trigger 3 DAGs: `Bronze Ingestion`, `Silver Transformation`, `Gold Stream Modeling`
    - Since the later job reads output of the former job (i.e., Silver reads output table of Bronze and Gold reads one from Silver). Bronze/Silver should output something before Silver/Gold is triggered

- Serving layer: Create the view specified at `./sql/serving_view.sql`
- Querying to tables at zones can be done using [DBCode](https://dbcode.io/). Setup connection to Postgres and Trino before querying

- Next, there're 2 sessions describing the behavior of the system in 2 views: Layer (batch, speed, serving) and Zone (Bronze, Silver, Gold). The Layer view is more general and is written down bellow. The Zone view is more detail and can be found at `./airflow/README.md`

### Layer view

#### Batch layer

![Batch pipeline](./flow/batch_pipeline.png)

- Pre-require knowledge for Delta lake
    - Upsert: [Basic](https://delta.io/blog/2023-02-14-delta-lake-merge/), [Advanced](https://delta.io/blog/delta-lake-upsert/)
    - Optimize: [Overview](https://delta.io/blog/delta-lake-optimize/), [Deletion Vectors](https://delta.io/blog/2023-07-05-deletion-vectors/), [Z Order](https://delta.io/blog/2023-06-03-delta-lake-z-order/)
    - Schema: [Schema enforcement](https://delta.io/blog/2022-11-16-delta-lake-schema-enforcement/), [Schema evolution](https://delta.io/blog/2023-02-08-delta-lake-schema-evolution/)

- Active dag `Bronze >> Silver >> Gold` to start batch layer
    - Read the dag file to track the batch process, which starts from **loading data from source to Bronze zone (append-only); cleans, transforms, deduplicate data then upsert into Silver zone; modelings data and upsert into Gold zone**.
    - Watermark will be updated in `./tracking`. Refer to `./tracking/*.backup` to understand how to initialize those watermarks
    - Silver and Gold zone are built based on `merge` operator, so **feel free to re-run** (unless the schema is heavily damage)
    - Gold zone is built by `dbt` as it's quite easy to setup. Read more about `dbt` [here](https://medium.com/@ahmealy/dbt-incremental-models-the-complete-guide-for-postgres-users-de18356a00a7)

- Use Trino to query delta table (setup via VSCode extension [DBCode](https://dbcode.io/) or just go straight into the container and query in there)

#### Speed layer (see `./airflow/README.md`)

- Pre-require knowledge for Stream processing
    - Spark Streaming: [Quick start](https://spark.apache.org/docs/latest/streaming/getting-started.html#structured-streaming-programming-guide-1), [Watermark in streaming](https://spark.apache.org/docs/latest/streaming/apis-on-dataframes-and-datasets.html#structured-streaming-programming-guide-1), [Spark - Kafka](https://spark.apache.org/docs/latest/streaming/structured-streaming-kafka-integration.html)
    - Watermark in streaming - [Explanation](https://www.youtube.com/watch?v=XjlKGvUt2dY)

- Trigger 3 dags: `Bronze Ingestion`, `Silver Transformation`, and `Gold Stream Modeling`. Note that
    - Since they are stream jobs, these dags will `awaitTermination`
    - `Bronze Ingestion` subscribes the CDC source. Since this job is append-only, **CDF (built-in CDC of DeltaLake) is not required**
    - `Silver Transformation`: Read stream from Bronze zone, **transform and write data to Silver zone with CDF on**. Thus, later job can capture `_change_type` such as `insert` and `update_postimage`
    - `Gold Stream Modeling`: Capture changelog from Silver zone, modeling them and overwrite to a temporary table (Postgres), then **upsert to the Serving table of speed layer**
    - These jobs read data from the previous one. Thus, they will raise errors if the previous source doesn't exist

- Speed layer generates a lot of small files, causing **small file problem** in DeltaLake. Optimization techniques (**deletion vector, compaction, z-oder, and vacuum**) are adopted to handle this problem

#### Serving layer

- A view in Postgres unions batch serving tables and stream serving table to serve newest data to end user
- The view is created as follows
    - Batch part: Query the whole batch historical data
    - Stream part: Only query from the part that batch layer doesn't include (yet)
