import os
from datetime import datetime, timezone
from random import randint

from delta.tables import DeltaTable
from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from bronze.ingest_bronze import SCHEMA as BRONZE_SCHEMA
from bronze.ingest_bronze import TABLE_BATCH as BRONZE_TABLE_BATCH
from bronze.ingest_bronze import TABLE_STREAM as BRONZE_TABLE_STREAM
from silver.silver_map import *
from silver.valid_silver import post_validate_silver
from utils.utils import _get_table_version, check_date, parse_args

load_dotenv('.env_file/infra.env')
load_dotenv('.env_file/app.env')

SCHEMA = "silver"

# batch
TABLE_BATCH  = "nyc_taxi"
SCHEMA_STORE = 'warehouse'
S3_SILVER_BATCH_PATH = os.path.join(os.getenv('S3_URI'), SCHEMA)

# stream
TABLE_STREAM = "nyc_taxi_stream"
S3_SILVER_STREAM_PATH = os.path.join(os.getenv('S3_URI'), f'{SCHEMA}_stream')
CHECKPOINT = '/home/nblong/Dropbox/Project/ops/ml-ops/spark/checkpoint/silver_stream'
# CHECKPOINT = os.path.join(os.getenv('S3_URI'), 'checkpoint', f'{SCHEMA}_stream')

def create_session(lh_host:str='localhost', checkpoint:str=CHECKPOINT) -> SparkSession:
    return (
        SparkSession.builder
        .appName('ingest data to silver zone')
        .config("spark.driver.memory", "8g")
        .config("spark.executor.memory", "8g")
        .config("spark.sql.session.timeZone", "UTC")

        # packages supporting read/write postgres, S3, hive (data lake)
        # and Kafka, avro (for streaming)
        .config(
            'spark.jars.packages',
            'org.apache.hadoop:hadoop-aws:3.4.2,'
            'software.amazon.awssdk:bundle:2.29.52,'
            'io.delta:delta-spark_4.1_2.13:4.3.0,'
        )

        # read/write S3
        .config("spark.hadoop.fs.s3a.access.key", os.getenv('AWS_ACCESS_KEY_ID'))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv('AWS_SECRET_ACCESS_KEY'))

        # delta lake support
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # # enable CDF (aka CDC) for all delta tables
        # .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")

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
        .config('spark.databricks.delta.properties.defaults.enableChangeDataFeed', 'true') # enable built-in CDC of delta lake (aka CDF)

        # create schema using spark (instead of using trino)
        .config("spark.sql.warehouse.dir", os.path.join(os.getenv('S3_URI'), SCHEMA_STORE))
        .enableHiveSupport()
        .getOrCreate()
    )

def _transform(df:DataFrame)->DataFrame:
    # cast some cols to their natural datatype
    cast_map = {
        'passenger_count': F.col('passenger_count').cast(T.IntegerType()),
        'pu_location_id' : F.col('pu_location_id').cast(T.IntegerType()),
        'do_location_id' : F.col('do_location_id').cast(T.IntegerType()),
    }
    df = df.withColumns(cast_map)

    # payment_type
    df = (
        df
        .withColumn('payment_type', F.lower(F.trim(F.col('payment_type'))))
        .replace(payment_map, subset='payment_type')
    )
    # payment_type_id
    df = (
        df.withColumn('payment_type_id', F.col('payment_type'))
        .replace(inverse_payment_map, subset='payment_type_id')
    )

    # drop outlier for `passenger_count`
    df = df.filter(
        (F.col('passenger_count')<7) &
        (F.col('passenger_count')!=0)
    )

    # drop outlier for `rate_code_id` and cast to int
    df = (
        df.filter(
            ((F.col('rate_code_id')>=1) & (F.col('rate_code_id')<=6)) | (F.col('rate_code_id')==99) |
            (F.col('rate_code_id').isNull())
        )
        .withColumn('rate_code_id', F.col('rate_code_id').cast('string'))
        .replace(rate_code_id_map, subset='rate_code_id')
        .withColumnRenamed('rate_code_id', 'rate_code')
    )
    # rate_code_id
    df = (
        df.withColumn('rate_code_id', F.col('rate_code'))
        .replace(inverse_rate_code_id_map, subset='rate_code_id')
    )

    # store_and_fwd_flag
    df = (
        df
        .withColumn('store_and_fwd_flag', F.lower(F.trim(F.col('store_and_fwd_flag'))))
        .replace(store_fwd_flag_map, subset='store_and_fwd_flag')
    )
    # store_and_fwd_flag_id
    df = (
        df.withColumn('store_and_fwd_id', F.col('store_and_fwd_flag'))
        .replace(inverse_store_fwd_flag_map, subset='store_and_fwd_id')
    )

    # assign int id for vendor (instead of using string)
    df = (
        df
        .withColumnRenamed('vendor_id', 'vendor_name')
        .withColumn('vendor_name', F.lower(F.trim(F.col('vendor_name'))))
        .replace(vendor_id_map, subset='vendor_name')
    )
    # vendor_id
    df = (
        df.withColumn('vendor_id', F.col('vendor_name'))
        .replace(inverse_vendor_id_map, subset='vendor_id')
    )

    # add audit fields of silver zone
    # batch_timestamp = datetime.now(timezone.utc)
    df = (
        df.withColumn('silver_created_datetime', F.current_timestamp())
        .withColumn('silver_updated_datetime', F.current_timestamp())
    )

    # create primary key (sorted columns for deterministic hash)
    # đáng nhẽ nên thêm `silver_created_datetime` vào `audit_cols` nhưng tôi muốn tạo ra các records bị update giả
    # nếu thêm `silver_created_datetime` thì data sẽ không được update nếu chạy lại (vì đây là dữ liệu tĩnh nên tôi muốn chạy lại để thử logic upsert)
    audit_cols = {'year', 'month', 'record_key', 'source_file', 'source_created_datetime', 'bronze_created_datetime', 'silver_updated_datetime'}
    df = df.withColumn(
        'content_hash',
        F.sha2(F.concat_ws(
            "||",
            *sorted(set(df.columns).difference(audit_cols))
        ), 256)
    )

    # de-dup
    return df.dropDuplicates(['content_hash'])

def _insert_batch(df:DataFrame, target_table:str, s3_path:str, cdf:bool=False)->int:
    cdf = 'true' if cdf else 'false'
    (
        df
        .repartition("year", "month") # prevent small files problem
        .write
        .option("delta.enableDeletionVectors", "true") # have deletion vectors on
        .option("delta.enableChangeDataFeed", cdf) # have cdf (built-in CDC of delta lake) on/off
        .format('delta')
        .partitionBy("year", "month")
        .mode("append")
        .option("path", s3_path)
        .saveAsTable(target_table)   # đăng ký luôn với Hive metastore
    )
    return df.count()

def _upsert_batch(spark:SparkSession, df:DataFrame, target_table:str):
    delta_table = DeltaTable.forName(spark, target_table)

    # upsert to delta table
    # những row thoả điều kiện sẽ được update các trường như định nghĩa
    # những row không thoả điều kiện sẽ được insert
    partitions = df.select("year", "month").distinct().collect()
    partition_filter = " or ".join([
        f"(target.year={r['year']} and target.month={r['month']})"
        for r in partitions
    ])
    target_before_count = delta_table.toDF().count()
    (
        delta_table.alias("target")
        .merge(
            source=df.alias("source"),
            condition=f"({partition_filter}) and (target.record_key = source.record_key)"
        )
        .whenMatchedUpdate(
            condition='target.content_hash != source.content_hash',
            set={
                col: f"source.{col}"
                for col in df.columns
                if col not in ['month', 'year', 'bronze_created_datetime', 'record_key', 'source_file', 'source_created_datetime', "silver_created_datetime"]
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
    target_after_count = delta_table.toDF().count()
    source_count = df.count()
    return target_before_count, target_after_count, source_count

def _save_batch(spark:SparkSession, df:DataFrame, target_table:str, s3_path:str, cdf:bool=False)->tuple[int, int]:
    """Save batch, but apply for both batch and streaming mode

    Args:
        spark (SparkSession): Spark session
        df (DataFrame): Saved dataframe
        target_table (str): Table name to be saved data into
        s3_path (str): S3 path of the table
        cdf (bool, optional): Enable CDF in delta table, only apply for streaming table. Defaults to False.

    Returns:
        tuple[int, int]: Return number of saved records and version of new table or -1 if there's no change in the table
    """
    previous_version = _get_table_version(spark, target_table)
    print(f"\nUpsert into table '{target_table}'")
    if previous_version == -1:
        source_count = _insert_batch(df, target_table, s3_path, cdf)
        print(f"\tInserted {source_count} rows")
    else:
        target_before_count, target_after_count, source_count = _upsert_batch(spark, df, target_table)
        print(f"\tTarget before operation: {target_before_count} rows. Target after operation: {target_after_count} rows")
        print(f"\tUpdated {target_before_count + source_count - target_after_count} rows")
        print(f"\tInserted {target_after_count - target_before_count} rows")
    current_version = _get_table_version(spark, target_table)
    return source_count, current_version if current_version != previous_version else -1

def ingest_silver(
    spark:SparkSession,
    watermark:str,
    source_table:str,
    target_table:str,
    s3_path:str
) -> tuple[str,int,int]:
    df = spark.read.table(source_table)
    df.createOrReplaceTempView('nyc_taxi')
    batch_start_ts = datetime.now(timezone.utc)
    df = spark.sql(f'''
        select * from nyc_taxi
        where bronze_created_datetime <= (
            select max(bronze_created_datetime)
            from (
                select * from nyc_taxi
                where bronze_created_datetime > timestamp '{watermark}'
                order by bronze_created_datetime asc
                limit {140_000*3}
            )
        ) and bronze_created_datetime > timestamp '{watermark}'
        and bronze_created_datetime < '{batch_start_ts!s}'
    ''')

    # process
    silver_df = _transform(df)

    # create schema in lakehouse
    spark.sql(f'create database if not exists {SCHEMA}')
    print(f'\nCreated {SCHEMA}\n')

    # save to delta lake
    if not silver_df.isEmpty():
        print(f"\nTable registered as '{target_table}' → {s3_path}\n")
        source_count, current_version = _save_batch(spark, silver_df, target_table, s3_path)
        new_watermark = str(silver_df.select(F.max('bronze_created_datetime')).collect()[0][0])
    else:
        source_count = 0
        current_version = -1
        new_watermark = watermark

    return (
        new_watermark,
        source_count,
        current_version
    )

def ingest_stream_silver(
    spark:SparkSession,
    source_table:str,
    target_table:str,
    target_s3_path:str,
    checkpoint:str
):
    bronze_df = (
        spark.readStream
        .format('delta')
        .option("startingVersion", '0') # `0,1,2,...` or `latest`. if checkpoint not found, read the whole table, else read from checkpoint
        .table(source_table)
    )
    silver_df = _transform(bronze_df)

    def _save_stream(batch_df:DataFrame, batch_id:int):
        """Save stream under `forEachBatch` function

        Args:
            batch_df (DataFrame): Batch
            batch_id (int): Batch id
        """
        if not batch_df.isEmpty():
            # save batch with CDF turned on
            source_count, current_version = _save_batch(spark, batch_df, target_table, target_s3_path, True)
            # nếu có thay đổi và hên xui (10%) thì kiểm tra
            if current_version != -1 and randint(1, 10) == 1:
                post_validate_silver(spark, target_table, source_count, current_version)

    # create schema in lakehouse
    spark.sql(f'create database if not exists {SCHEMA}')
    print(f'\nCreated {SCHEMA}\n')

    # save to S3
    query = (
        silver_df.writeStream
        .foreachBatch(_save_stream)
        .trigger(processingTime='55 seconds')
        .option("checkpointLocation", checkpoint)
        .start()
    )
    print(f"\nTable registered as '{target_table}' → {target_s3_path}\n")
    query.awaitTermination()

def main(lh_host:str='localhost'):
    params = parse_args()
    spark = create_session(lh_host)

    if params['mode'] == 'batch':
        # read the tracking file to get the previous_last_date
        with open('./tracking/track_silver', 'r') as fi:
            list_dates = fi.readlines()
            previous_watermark = list_dates[0].strip()
            fi.close()

        print(f'\n=== Start ingest from bronze_created_datetime (watermark) > {previous_watermark} ===\n')
        source_table = f'{BRONZE_SCHEMA}.{BRONZE_TABLE_BATCH}'
        target_table = f'{SCHEMA}.{TABLE_BATCH}'
        current_watermark, source_count, current_version = ingest_silver(
            spark=spark,
            watermark=previous_watermark,
            source_table=source_table,
            target_table=target_table,
            s3_path=S3_SILVER_BATCH_PATH
        )

        if (
            current_version != -1
            and current_watermark != previous_watermark
            and check_date(current_watermark)
        ):
            print(f'\n=== New bronze_created_datetime (watermark): {current_watermark} ===\n')

            # post-test
            post_validate_silver(spark, target_table, source_count, current_version)

            # update current_last_date to the tracking file
            list_dates.insert(0, f'{current_watermark}\n')
            with open('./tracking/track_silver', 'w') as fout:
                fout.writelines(list_dates)
                fout.close()

    elif params['mode'] == 'stream':
        source_table = f'{BRONZE_SCHEMA}.{BRONZE_TABLE_STREAM}'
        target_table = f'{SCHEMA}.{TABLE_STREAM}'
        ingest_stream_silver(
            spark=spark,
            source_table=source_table,
            target_table=target_table,
            target_s3_path=S3_SILVER_STREAM_PATH,
            checkpoint=CHECKPOINT
        )

if __name__=='__main__':
    # main()
    main(lh_host=os.getenv('LH_METASTORE_HOST'))
