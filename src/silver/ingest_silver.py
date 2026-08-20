import os
from datetime import datetime, timezone

from delta.tables import DeltaTable
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from bronze.ingest_bronze import SCHEMA as BRONZE_SCHEMA
from bronze.ingest_bronze import TABLE as BRONZE_TABLE
from silver.silver_map import (
    payment_map,
    rate_code_id_map,
    store_fwd_flag_map,
    vendor_id_map,
)
from silver.valid_silver import post_validate_silver
from utils.utils import check_date

load_dotenv('.env_file/infra.env')
load_dotenv('.env_file/app.env')

SCHEMA = "silver"
S3_SILVER_PATH = os.path.join(os.getenv('S3_URI'), SCHEMA)
TABLE  = "nyc_taxi"
SCHEMA_STORE = 'warehouse'

def create_session(lh_host:str) -> SparkSession:
    return (
        SparkSession.builder
        .appName('ingest data to silver zone')
        .config("spark.driver.memory", "8g")
        .config("spark.executor.memory", "8g")
        .config("spark.sql.session.timeZone", "UTC")
        # workaround for Spark 4.1.x CollapseProject optimizer bug
        .config("spark.sql.optimizer.excludedRules", "org.apache.spark.sql.catalyst.optimizer.CollapseProject")
        # delta lake support
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # read/write S3
        .config("spark.hadoop.fs.s3a.access.key", os.getenv('AWS_ACCESS_KEY_ID'))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv('AWS_SECRET_ACCESS_KEY'))
        # jar files supporting read/write postgres, trino, S3, hive (delta table for lake house)
        .config(
            'spark.jars',
            """
                ./spark/jar/hadoop-aws-3.4.2.jar,
                ./spark/jar/bundle-2.29.52.jar,
                ./spark/jar/delta-spark_4.1_2.13-4.3.0.jar,
                ./spark/jar/delta-kernel-api-4.3.0.jar,
                ./spark/jar/delta-kernel-defaults-4.3.0.jar,
                ./spark/jar/delta-storage-4.3.0.jar,
            """
        )
        # Hive metastore
        .config("spark.hadoop.hive.metastore.uris", f"thrift://{lh_host}:{9083 if lh_host!= 'localhost' else 9084}")
        .config("spark.sql.catalogImplementation", "hive")
        # create schema using spark (instead of using trino)
        .config("spark.sql.warehouse.dir", os.path.join(os.getenv('S3_URI'), SCHEMA_STORE))
        .enableHiveSupport()
        .getOrCreate()
    )

def ingest_silver(spark:SparkSession, watermark:str) -> str:
    df = spark.read.table(f'{BRONZE_SCHEMA}.{BRONZE_TABLE}')
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

    if df.count() == 0:
        return watermark, 0, None

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

    # store_and_fwd_flag
    df = (
        df
        .withColumn('store_and_fwd_flag', F.lower(F.trim(F.col('store_and_fwd_flag'))))
        .replace(store_fwd_flag_map, subset='store_and_fwd_flag')
    )

    # assign int id for vendor (instead of using string)
    df = (
        df
        .withColumnRenamed('vendor_id', 'vendor')
        .withColumn('vendor', F.lower(F.trim(F.col('vendor'))))
        .replace(vendor_id_map, subset='vendor')
    )

    # create schema in lakehouse
    spark.sql(f'create database if not exists {SCHEMA}')
    print(f'\nCreated {SCHEMA}\n')

    # add audit fields of silver zone
    batch_timestamp = datetime.now(timezone.utc)
    df = (
        df.withColumn('silver_created_datetime', F.lit(batch_timestamp).cast('timestamp'))
        .withColumn('silver_updated_datetime', F.lit(batch_timestamp).cast('timestamp'))
    )

    # create primary key (sorted columns for deterministic hash)
    # đáng nhé nên thêm `silver_created_datetime` vào `audit_cols` nhưng tôi muốn tạo ra các records bị update giả
    # nếu thêm `silver_created_datetime` thì data sẽ không được update nếu chạy lại (vì đây là dữ liệu tĩnh nên tôi muốn chạy lại để thử logic upsert)
    audit_cols = {'year', 'month', 'source_file', 'source_created_datetime', 'bronze_created_datetime', 'silver_updated_datetime'}
    df = df.withColumn(
        'content_hash',
        F.sha2(F.concat_ws(
            "||",
            *sorted(set(df.columns).difference(audit_cols))
        ), 256)
    )

    # de-dup
    df = (
        df
        # .drop('bronze_created_datetime')
        .dropDuplicates(['content_hash'])
    )

    table_name = f'{SCHEMA}.{TABLE}'
    if spark.catalog.tableExists(table_name):
        delta_table = DeltaTable.forName(spark, table_name)

        # get current version
        current_version = (
            delta_table.history(limit=1)
            .select('version').collect()[0][0]
        )

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
        print(f"""
Table registered as '{table_name}' → {S3_SILVER_PATH}
\tTarget before operation: {target_before_count} rows. Target after operation: {target_after_count} rows
\tUpdated {target_before_count + source_count - target_after_count} rows
\tInserted {target_after_count - target_before_count} rows
""")
    else:
        current_version = -1
        # save to lake house
        (
            df
            .repartition("year", "month") # prevent small files problem
            .write
            .option("delta.enableDeletionVectors", "true") # have deletion vectors on
            .format('delta')
            .partitionBy("year", "month")
            .mode("append")
            .option("path", S3_SILVER_PATH)
            .saveAsTable(table_name)   # đăng ký luôn với Hive metastore
        )

        source_count = df.count()
        print(f"""
Table registered as '{table_name}' → {S3_SILVER_PATH}
\tInserted {source_count} rows
""")

    return (
        str(df.select(F.max('bronze_created_datetime')).collect()[0][0]),
        source_count,
        current_version,
    )

# trigger manually
def main(lh_host:str='localhost'):
    # read the tracking file to get the previous_last_date
    with open('./tracking/track_silver', 'r') as fi:
        list_dates = fi.readlines()
        previous_last_date = list_dates[0].strip()
        fi.close()

    if check_date(previous_last_date):
        print(f'\n=== Start ingest from bronze_created_datetime (watermark) > {previous_last_date} ===\n')
        spark = create_session(lh_host)
        current_last_date, source_count, current_version = ingest_silver(spark, previous_last_date)

        if current_last_date != previous_last_date and check_date(current_last_date):
            print(f'\n=== New bronze_created_datetime (watermark): {current_last_date} ===\n')

            # post-test
            if source_count != 0:
                post_validate_silver(spark, source_count, current_version+1)

            # update current_last_date to the tracking file
            list_dates.insert(0, f'{current_last_date}\n')
            with open('./tracking/track_silver', 'w') as fout:
                fout.writelines(list_dates)
                fout.close()

if __name__=='__main__':
    main(lh_host=os.getenv('LH_METASTORE_HOST'))
