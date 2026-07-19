from pyspark.sql import SparkSession
from pyspark.sql import functions as F, types as T
from delta.tables import DeltaTable

import os
from dotenv import load_dotenv
from datetime import datetime, timezone

from silver.silver_map import payment_map, store_fwd_flag_map, vendor_id_map
from silver.valid_silver import post_valid_silver
from utils.utils import check_date

load_dotenv('.env_file/infra.env')
load_dotenv('.env_file/app.env')

S3_SILVER_PATH = "s3a://flezi-data/silver/"
SCHEMA = "silver"
TABLE  = "nyc_taxi"
SCHEMA_STORE = 'warehouse'

def create_session() -> SparkSession:
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
                ./spark/jar/trino-jdbc-480.jar,
                ./spark/jar/hadoop-aws-3.4.2.jar,
                ./spark/jar/bundle-2.29.52.jar,
                ./spark/jar/delta-spark_4.1_2.13-4.3.0.jar,
                ./spark/jar/delta-kernel-api-4.3.0.jar,
                ./spark/jar/delta-kernel-defaults-4.3.0.jar,
                ./spark/jar/delta-storage-4.3.0.jar,
            """
        )
        # Hive metastore
        .config("spark.hadoop.hive.metastore.uris", f"thrift://{os.getenv('LH_METASTORE_HOST')}:9083")
        .config("spark.sql.catalogImplementation", "hive")
        # create schema using spark (instead of using trino)
        .config("spark.sql.warehouse.dir", f"s3a://flezi-data/{SCHEMA_STORE}/")
        .enableHiveSupport()
        .getOrCreate()
    )

def ingest_silver(spark:SparkSession, watermark:str) -> str:
    trino_url = "jdbc:trino://trino:8080"
    trino_user = "trino"
    trino_password = ""
    source_table = "datalake.bronze.nyc_taxi"

    df = (
        spark.read
        .option("pushDownPredicate", "false")
        .format("jdbc")
        .option("url", trino_url)
        .option("driver", "io.trino.jdbc.TrinoDriver")
        .option("dbtable", source_table)
        .option("user", trino_user)
        .option("password", trino_password)
        .load()
    )

    df.createOrReplaceTempView('nyc_taxi')

    # limit for 3 months, avoid out of mem
    # ban đầu chỉ định lấy 140_000*3 rows
    # nhưng có thể có TH ngày created mới nhất trong đó có nhiều giá trị (và chưa được lấy vào vì limit 140_000)
    # nên phải lấy toàn toàn bộ các rows > watermark và <= ngày lớn nhất trong mớ 140_000*3 rows định lấy ban đầu
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
        ) and bronze_created_datetime > timestamp '{watermark}';
    ''').cache()  # cache to avoid re-reading from Trino on each action

    if df.count() == 0:
        return watermark, 0, None

    # transfer: cast some columns, replace data within some columns
    cast_map = {
        'passenger_count': F.col('passenger_count').cast(T.IntegerType()),
        'pu_location_id' : F.col('pu_location_id').cast(T.IntegerType()),
        'do_location_id' : F.col('do_location_id').cast(T.IntegerType())
    }
    df = df.withColumns(cast_map)
    df = (
        df
        .withColumn('payment_type', F.lower(F.trim(F.col('payment_type'))))
        .replace(payment_map, subset='payment_type')
    )
    df = (
        df
        .withColumn('store_and_fwd_flag', F.lower(F.trim(F.col('store_and_fwd_flag'))))
        .replace(store_fwd_flag_map, subset='store_and_fwd_flag')
    )
    df = (
        df
        .withColumn('vendor_id', F.lower(F.trim(F.col('vendor_id'))))
        .replace(vendor_id_map, subset='vendor_id')
    )

    # create primary key
    df = df.withColumn(
        'trip_key',
        F.sha2(F.concat_ws(
            "||",
            "pickup_datetime", "dropoff_datetime", "passenger_count", "trip_distance", "pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude"
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
        delta_table = DeltaTable.forName(spark, table_full_name)
        target_before_count = delta_table.toDF().count()
        (
            delta_table.alias("target")
            .merge(
                df.alias("source"),
                "target.trip_key = source.trip_key"
            )
            .whenMatchedUpdate(set={
                col: f"source.{col}"
                for col in df.columns
                if col != "silver_created_datetime"
            })
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
            .repartition("year", "month")
            .write
            .format('delta')
            .partitionBy("year", "month")
            .mode("append")
            .option("path", S3_SILVER_PATH)
            .saveAsTable(table_full_name)   # đăng ký luôn với Hive metastore
        )

        print(f"""
Table registered as '{table_full_name}' → {S3_SILVER_PATH}
\tInserted {source_count} rows
""")

    return str(df.select(F.max('bronze_created_datetime')).collect()[0][0]), source_count, str(batch_timestamp).split('+')[0]

# trigger manually
def main():
    spark = create_session()
    print(ingest_silver(spark, '2026-06-30 16:04:38.483'))

if __name__=='__main__':
    # main()

    # read the tracking file to get the previous_last_date
    with open('./tracking/track_silver', 'r') as fi:
        list_dates = fi.readlines()
        previous_last_date = list_dates[0].strip()
        fi.close()

    if check_date(previous_last_date):
        print(f'\n=== Start ingest from {previous_last_date} ===\n')
        spark = create_session()
        current_last_date, ingested_count, silver_updated_datetime = ingest_silver(spark, previous_last_date)

        if current_last_date != previous_last_date and check_date(current_last_date):
            print(f'\n=== Last date: {current_last_date} ===\n')
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
