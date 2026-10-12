import os
from datetime import datetime, timezone
from random import randint

import pyspark.sql.functions as F
from confluent_kafka.schema_registry import SchemaRegistryClient
from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.avro.functions import from_avro

from bronze.valid_bronze import post_validate_bronze
from utils.utils import _get_table_version, check_date, parse_args

load_dotenv('.env_file/infra.env')
load_dotenv('.env_file/app.env')

SCHEMA = "bronze"

# batch
TABLE_BATCH  = "nyc_taxi"
SCHEMA_STORE = 'warehouse'
S3_BRONZE_BATCH_PATH = os.path.join(os.getenv('S3_URI'), SCHEMA)

# stream
TABLE_STREAM  = "nyc_taxi_stream"
BOOTSTRAP_SERVERS = 'localhost:9092'
SCHEMA_REGISTRY = 'http://localhost:8081'
S3_BRONZE_STREAM_PATH = os.path.join(os.getenv('S3_URI'), f'{SCHEMA}_stream')
TOPIC = 'source.public.nyc_taxi_stream'
CHECKPOINT = '/home/nblong/Dropbox/Project/ops/ml-ops/spark/checkpoint/bronze_stream'

def create_session(lh_host:str='localhost', checkpoint:str=CHECKPOINT) -> SparkSession:
    """Create spark session for stream and batch processing

    Args:
        lh_host (str): Metadata store endpoint for delta lake. Default to 'localhost'
        checkpoint (str, optional): Checkpoint for stream processing. Defaults to '/home/nblong/Dropbox/Project/ops/ml-ops/spark/checkpoint'.

    Returns:
        SparkSession: Spark Session
    """
    return (
        SparkSession.builder
        .appName('ingest data to bronze zone')
        .config("spark.driver.memory", "8g")
        .config("spark.executor.memory", "8g")
        .config("spark.sql.session.timeZone", "UTC")

        # packages supporting read/write postgres, S3, hive (data lake)
        # and Kafka, avro (for streaming)
        .config(
            'spark.jars.packages',
            'org.postgresql:postgresql:42.7.11,'
            'org.apache.hadoop:hadoop-aws:3.4.2,'
            'software.amazon.awssdk:bundle:2.29.52,'
            'io.delta:delta-spark_4.1_2.13:4.3.0,'
            'org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,'
            'org.apache.spark:spark-avro_2.13:4.1.1,'
        )

        # read/write S3
        .config("spark.hadoop.fs.s3a.access.key", os.getenv('AWS_ACCESS_KEY_ID'))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv('AWS_SECRET_ACCESS_KEY'))

        # delta lake support
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        # allow rewrite partitions
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")

        # Hive metastore
        .config("spark.hadoop.hive.metastore.uris", f"thrift://{lh_host}:{9083 if lh_host!= 'localhost' else 9084}")
        .config("spark.sql.catalogImplementation", "hive")

        # workaround for Spark 4.1.x CollapseProject optimizer bug
        .config("spark.sql.optimizer.excludedRules", "org.apache.spark.sql.catalyst.optimizer.CollapseProject")

        # stream support
        .config('spark.sql.streaming.checkpointLocation', checkpoint)
        .config('spark.sql.streaming.forceDeleteTempCheckpointLocation', 'true')

        # create schema using spark (instead of using trino)
        .config("spark.sql.warehouse.dir", os.path.join(os.getenv('S3_URI'), SCHEMA_STORE))
        .enableHiveSupport()
        .getOrCreate()
    )

def _transform(df:DataFrame)->DataFrame:
    """Transform the dataframe. Just add a record key as PK and partition by month and year of event time

    Args:
        df (DataFrame): Input dataframe

    Returns:
        DataFrame: Output dataframe
    """
    # create key
    df = df.withColumn(
        'record_key',
        F.sha2(F.concat_ws(
            "||",
            *sorted(set(df.columns).difference({'source_file', 'source_created_datetime'}))
        ), 256)
    )

    # convert `source_created_at` to timestamp (in case of Kafka)
    df = df.withColumn('source_created_datetime', F.col('source_created_datetime').cast('timestamp'))

    # partition pickup_datetime by month and year
    # bronze_created_datetime = datetime.now(timezone.utc)
    return (
        df
        .withColumn('bronze_created_datetime', F.current_timestamp())
        .withColumn('year', F.substring('source_file', 17, 4).cast('int'))
        .withColumn('month', F.substring('source_file', 22, 2).cast('int'))
    )

def _save_batch(spark:SparkSession, df:DataFrame, target_table:str, s3_path:str)->tuple[int,int]:
    """Save the dataframe to S3 with delta format, partitioned by month and year of event time

    Args:
        spark (SparkSession): Spark Session
        df (DataFrame): The dataframe
        table_name (str): Target table
        s3_path (str): Target path on S3
        source_count(int): df.count()

    Returns:
        int: Version of the current table
    """
    previous_version = _get_table_version(spark, target_table)
    (
        df.repartition("year", "month") # prevent small files problem
        .write
        .option("delta.enableDeletionVectors", "true") # have deletion vectors on
        .format('delta')
        .partitionBy("year", "month")
        .mode("append")
        .option("path", s3_path)
        .saveAsTable(target_table)   # đăng ký luôn với Hive metastore
    )
    source_count = df.count()
    print(f"Table registered as '{target_table}' → {s3_path}")
    print(f"\tInserted {source_count} rows\n")

    current_version = _get_table_version(spark, target_table)

    return source_count, current_version if current_version != previous_version else -1

def ingest_bronze(
        spark: SparkSession,
        db_host:str,
        db_table:str,
        watermark:str,
        target_table:str,
        s3_path:str
    ) -> tuple[str, int, int]:
    """Ingest data from source to bronze zone

    Args:
        spark (SparkSession): SparkSession which can access to DB and Data lake
        watermark (str): Latest `source_created_datetime` in DB

    Returns:
        str: New watermark
    """
    # load data from db
    df = (
        spark.read.format("jdbc")
        .option("driver", "org.postgresql.Driver")
        .option("url", f"jdbc:postgresql://{db_host}:{os.getenv('COM_DB_PORT')}/{os.getenv('COM_DB_NAME')}")
        .option("dbtable", db_table)
        .option("user", os.getenv("COM_DB_USER"))
        .option("password", os.getenv("COM_DB_PASSWORD"))
        .load()
    )

    batch_start_ts = datetime.now(timezone.utc)
    df.createOrReplaceTempView('nyc_taxi')

    # lấy data trong khoảng (watermark, batch_start_ts) để nó không bị khác snapshot do lazy evaluation của spark
    # giới hạn tầm 140_000*3 rows để đỡ nổ
    # phải lấy thêm những TH có cùng source_created_datetime nhưng bị giới hạn bởi 140_000 để watermark lần tới được chính xác
    # phải update cả silver & gold nữa
    df = spark.sql(f'''
        select * from nyc_taxi
        where source_created_datetime >= timestamp '{watermark}'
        and source_created_datetime < '{batch_start_ts!s}'
        limit {140_000*3}
    ''')

    # process
    partition_bronze_df = _transform(df)

    # create schema in lakehouse and define table_name
    spark.sql(f'create database if not exists {SCHEMA}')
    print(f'\nCreated {SCHEMA}\n')

    # save to delta lake
    if not partition_bronze_df.isEmpty():
        source_count, current_version = _save_batch(spark, partition_bronze_df, target_table, s3_path)
        new_watermark = str(df.select(F.max('source_created_datetime')).collect()[0][0])
    else:
        source_count = 0
        current_version = -1
        new_watermark = watermark

    return (
        new_watermark,
        source_count,
        current_version
    )

def _get_schema_from_registry(subject:str, schema_registry_url:str="http://localhost:8081")->str:
    """Get the schema from schema-registry

    Args:
        subject (str): {topic}-{value/key}. The name specifying the key/value format for msg in topic
        schema_registry_url (str, optional): Schema-registry endpoint. Defaults to "http://localhost:8081".

    Returns:
        str: dict-form of schema and other information got from schema-registry
    """
    client = SchemaRegistryClient({"url": schema_registry_url})
    return client.get_latest_version(subject).to_dict()

def _parse_cdc_event(df:DataFrame, topic:str, schema_registry_url:str='http://localhost:8081'):
    """Parse Avro format to readable form and put everything to a Dataframe. Req: The 

    Args:
        df (DataFrame): The raw dataframe
        topic (str): Topic to get schema from registry
        schema_registry_url (str, optional): Schema endpoint. Defaults to 'http://localhost:8081'.

    Returns:
        Dataframe: The parsed dataframe
    """
    # Fetch value schema from Schema Registry
    value_subject = f'{topic}-value'

    try:
        value_schema = _get_schema_from_registry(value_subject, schema_registry_url)['schema']

        # Deserialize Avro data
        # Skip first 5 bytes (Confluent wire format: magic byte + 4-byte schema ID)
        decoded_df = df.select(
            from_avro(
                F.expr("substring(value, 6)"),  # bỏ 5 bytes đầu (1 magic + 4 schema_id)
                value_schema
            ).alias("payload"),
            F.col("timestamp").alias("kafka_ts"),
            F.col("partition"),
            F.col("offset"),
        )

        # Extract fields - after ExtractNewRecordState transform, Debezium adds __op, __table
        # Use recorded_at as event_time (it's already a ZonedTimestamp string from Debezium)
        return decoded_df.select(
            F.col('payload.*')
        )

    except Exception as e:
        print(f'Error parsing Avro data: {e}')
        raise

def ingest_stream_bronze(
    spark:SparkSession,
    source_topic:str,
    target_table:str,
    target_s3_path:str,
    checkpoint:str
):
    df_stream = (
        spark.readStream
        .format('kafka')
        .option('kafka.bootstrap.servers', BOOTSTRAP_SERVERS)
        .option('subscribe', source_topic)
        .option('startingOffsets', 'latest') # `earliest` hoặc `latest`
        .option('failOnDataLoss', 'false')
        .load()
    )

    # parse raw data to readable form and transform
    parsed_df = _parse_cdc_event(df_stream, source_topic, SCHEMA_REGISTRY)
    df = (
        parsed_df
        .drop('__deleted', '__op', '__table')
        .withColumn('pickup_datetime', (F.col('pickup_datetime')/1_000_000).cast('timestamp'))
        .withColumn('dropoff_datetime', (F.col('dropoff_datetime')/1_000_000).cast('timestamp'))
    )
    partition_bronze_df = _transform(df)

    def _save_stream(batch_df:DataFrame, batch_id:int):
        if not batch_df.isEmpty():
            source_count, current_version = _save_batch(spark, batch_df, target_table, target_s3_path)
            # nếu có thay đổi và hên xui (20%) thì kiểm tra
            if current_version != -1 and randint(1, 10)%5 == 0:
                post_validate_bronze(spark, target_table, source_count, current_version)

    # create schema in lakehouse
    spark.sql(f'create database if not exists {SCHEMA}')
    print(f'\nCreated {SCHEMA}\n')

    # save to S3
    query = (
        partition_bronze_df.writeStream
        .foreachBatch(_save_stream)
        .trigger(processingTime='30 seconds')
        .option('checkpointLocation', checkpoint)
        .start()
    )
    print(f"Table registered as '{target_table}' → {target_s3_path}")
    query.awaitTermination()

def main(lh_host:str='localhost', db_host:str='localhost'):
    params = parse_args()
    spark = create_session(lh_host=lh_host)

    if params['mode'] == 'batch':
        # read the tracking file to get the previous_last_date
        with open('./tracking/track_bronze', 'r') as fi:
            list_dates = fi.readlines()
            previous_watermark = list_dates[0].strip()
            fi.close()

        print(f'\n=== Start ingest from: source_created_datetime (watermark) >= {previous_watermark} ===\n')

        target_table = f'{SCHEMA}.{TABLE_BATCH}'

        current_watermark, source_count, current_version = ingest_bronze(
            spark=spark,
            db_host=db_host,
            db_table='public.nyc_taxi',
            watermark=previous_watermark,
            target_table=target_table,
            s3_path=S3_BRONZE_BATCH_PATH
        )

        if (
            current_version != -1
            and current_watermark != previous_watermark
            and check_date(current_watermark)
        ):
            print(f'\n=== New source_created_datetime (watermark) = {current_watermark} ===\n')

            # post-test
            post_validate_bronze(spark, target_table, source_count, current_version)

            # update current_last_date to the tracking file
            list_dates.insert(0, f'{current_watermark}\n')
            with open('./tracking/track_bronze', 'w') as fout:
                fout.writelines(list_dates)
                fout.close()

    elif params['mode'] == 'stream':
        target_table = f'{SCHEMA}.{TABLE_STREAM}'
        ingest_stream_bronze(
            spark=spark,
            source_topic=TOPIC,
            target_table=target_table,
            target_s3_path=S3_BRONZE_STREAM_PATH,
            checkpoint=CHECKPOINT
        )

if __name__=='__main__':
    # main()
    main(
        lh_host=os.getenv('LH_METASTORE_HOST'),
        db_host=os.getenv('COM_DB_HOST')
    )

