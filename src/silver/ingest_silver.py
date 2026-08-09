import os
from datetime import datetime, timezone

from delta.tables import DeltaTable
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from bronze.ingest_bronze import SCHEMA as BRONZE_SCHEMA
from bronze.ingest_bronze import TABLE as BRONZE_TABLE
from silver.silver_map import payment_map, store_fwd_flag_map, vendor_id_map
from silver.valid_silver import post_valid_silver
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
        'do_location_id' : F.col('do_location_id').cast(T.IntegerType())
    }
    df = df.withColumns(cast_map)

    # change `payment_type` to `payment_type_id` and cast to int
    df = (
        df
        .withColumn('payment_type_id', F.lower(F.trim(F.col('payment_type'))))
        .replace(payment_map, subset='payment_type_id')
        .withColumn('payment_type_id', F.col('payment_type_id').cast('int'))
        .drop('payment_type')
    )

    # drop outlier for `passenger_count`
    df = df.filter(
        (F.col('passenger_count')<7) &
        (F.col('passenger_count')!=0)
    )

    # drop outlier for `rate_code_id` and cast to int
    df = (
        df.filter(
            ((F.col('rate_code_id')>=1.0) & (F.col('rate_code_id')<=6.0)) | (F.col('rate_code_id')==99.0) |
            (F.col('rate_code_id').isNull())
        )
        .withColumn('rate_code_id', F.col('rate_code_id').cast("int"))
    )

    # store_and_fwd_flag: 1/0 instead of Yes/No
    df = (
        df
        .withColumn('store_and_fwd_id', F.lower(F.trim(F.col('store_and_fwd_flag'))))
        .replace(store_fwd_flag_map, subset='store_and_fwd_id')
        .withColumn('store_and_fwd_id', F.col('store_and_fwd_id').cast('int'))
        .drop('store_and_fwd_flag')
    )

    # assign int id for vendor (instead of using string)
    df = (
        df
        .withColumn('vendor_id', F.lower(F.trim(F.col('vendor_id'))))
        .replace(vendor_id_map, subset='vendor_id')
        .withColumn('vendor_id', F.col('vendor_id').cast('int'))
    )

    # create primary key (sorted columns for deterministic hash)
    audit_cols = {'year', 'month', 'day', 'hour', 'minute', 'source_file', 'source_created_datetime', 'bronze_created_datetime'}
    df = df.withColumn(
        'trip_key',
        F.sha2(F.concat_ws(
            "||",
            *sorted(set(df.columns).difference(audit_cols))
        ), 256)
    )

    # de-dup
    df = (
        df
        # .drop('bronze_created_datetime')
        .dropDuplicates(['trip_key'])
    )

    source_count = df.count()

    # create schema in trino
    spark.sql(f'create database if not exists {SCHEMA}')
    print(f'\nCreated {SCHEMA}\n')

    # add watermark
    batch_timestamp = datetime.now(timezone.utc)
    df = (
        df.withColumn('silver_created_datetime', F.lit(batch_timestamp).cast('timestamp'))
        .withColumn('silver_updated_datetime', F.lit(batch_timestamp).cast('timestamp'))
    )

    table_full_name = f'{SCHEMA}.{TABLE}'
    if spark._jsparkSession.catalog().tableExists(table_full_name):
        # upsert to delta table
        # khi upsert theo logic này, sẽ luôn luôn có 1 lượng row = source_count bị effected trong target table
        # những row thoả điều kiện sẽ có updated_datetime mới (ít nhất là vậy)
        # những row không thoả điều kiện sẽ được insert
        # điều này dẫn đến logic khi test
        partitions = df.select("year", "month", "day", "hour", "minute").distinct().collect()
        partition_filter = " or ".join([
            f"(target.year={r['year']} and target.month={r['month']} and target.day={r['day']} and target.hour={r['hour']} and target.minute={r['minute']})"
            for r in partitions
        ])
        delta_table = DeltaTable.forName(spark, table_full_name)
        target_before_count = delta_table.toDF().count()
        (
            delta_table.alias("target")
            .merge(
                source=df.alias("source"),
                condition=f"({partition_filter}) and (target.trip_key = source.trip_key)"
            )
            .whenMatchedUpdate(
                set={
                    col: f"source.{col}"
                    for col in df.columns
                    if col != "silver_created_datetime"
                }
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
        target_after_count = delta_table.toDF().count()

        print(f"""
Table registered as '{table_full_name}' → {S3_SILVER_PATH}
\tUpdated {target_before_count + source_count - target_after_count} rows
\tInserted {target_after_count - target_before_count} rows
""")
    else:
        # save to lake house
        (
            df
            .repartition("year", "month", "day", "hour", "minute") # prevend small files problem
            .write
            .option("delta.enableDeletionVectors", "true") # have deletion vectors on
            .format('delta')
            .partitionBy("year", "month", "day", "hour", "minute")
            .mode("append")
            .option("path", S3_SILVER_PATH)
            .saveAsTable(table_full_name)   # đăng ký luôn với Hive metastore
        )

        print(f"""
Table registered as '{table_full_name}' → {S3_SILVER_PATH}
\tInserted {source_count} rows
""")

    return str(df.select(F.max('bronze_created_datetime')).collect()[0][0]), source_count, str(batch_timestamp)

# trigger manually
def main():
    spark = create_session('localhost')
    print(ingest_silver(spark, '2026-06-30 16:04:38.483'))

if __name__=='__main__':
    # main()

    # read the tracking file to get the previous_last_date
    with open('./tracking/track_silver', 'r') as fi:
        list_dates = fi.readlines()
        previous_last_date = list_dates[0].strip()
        fi.close()

    if check_date(previous_last_date):
        print(f'\n=== Start ingest from bronze_created_datetime (watermark) > {previous_last_date} ===\n')
        spark = create_session(os.getenv('LH_METASTORE_HOST'))
        current_last_date, ingested_count, silver_updated_datetime = ingest_silver(spark, previous_last_date)

        if current_last_date != previous_last_date and check_date(current_last_date):
            print(f'\n=== New bronze_created_datetime (watermark): {current_last_date} ===\n')
            list_dates.insert(0, f'{current_last_date}\n')
            if len(list_dates) > 500:
                list_dates.pop()

            # update current_last_date to the tracking file
            with open('./tracking/track_silver', 'w') as fout:
                fout.writelines(list_dates)
                fout.close()

            # post-test
            if ingested_count != 0:
                post_valid_silver(spark, silver_updated_datetime, ingested_count)
