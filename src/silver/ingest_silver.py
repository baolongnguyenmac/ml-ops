from pyspark.sql import SparkSession
from pyspark.sql import functions as F, types as T

import os
from dotenv import load_dotenv

from silver.silver_map import payment_map, store_fwd_flag_map, vendor_id_map
from utils.utils import check_date

load_dotenv('.env_file/infra.env')
load_dotenv('.env_file/app.env')

S3_BRONZE_PATH = "s3a://flezi-data/silver/"
SCHEMA = "taxi_data_silver"
TABLE  = "nyc_taxi"
SCHEMA_STORE = 'warehouse_silver'

def create_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName('ingest data to silver zone')
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
                ./spark/jar/delta-spark_4.1_2.13-4.2.0.jar,
                ./spark/jar/delta-kernel-api-4.2.0.jar,
                ./spark/jar/delta-kernel-defaults-4.2.0.jar,
                ./spark/jar/delta-storage-4.2.0.jar,
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

def ingest_silver(spark:SparkSession, previous_last_date:str) -> str:
    trino_url = "jdbc:trino://trino:8080"
    trino_user = "trino"
    trino_password = ""
    source_table = "datalake.taxi_data.nyc_taxi"

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

    # filter new records
    df = df.filter(
        df['pickup_datetime'] > previous_last_date
    )

    if df.count() == 0:
        return previous_last_date

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

    # create schema in trino
    spark.sql(f'create database if not exists {SCHEMA}')

    # save to lake house
    table_exists = spark.catalog.tableExists(f"{SCHEMA}.{TABLE}")
    writer = (
        df
        .write
        .format('delta')
        .mode("append")
        .option("path", S3_BRONZE_PATH)
    )
    if not table_exists:
        writer = writer.partitionBy("year", "month")
    writer.saveAsTable(f"{SCHEMA}.{TABLE}")   # đăng ký luôn với Hive metastore

    print(f"Done. Table registered as '{SCHEMA}.{TABLE}' → {S3_BRONZE_PATH}")
    return str(df.select(F.max('pickup_datetime')).collect()[0][0])

# trigger manually
def main():
    spark = create_session()
    ingest_silver(spark, '2000-01-01 00:00:00')

if __name__=='__main__':
    # read the tracking file to get the previous_last_date
    with open('./tracking/track_silver', 'r') as fi:
        list_dates = fi.readlines()
        previous_last_date = list_dates[0].strip()
        fi.close()

    if check_date(previous_last_date):
        print(f'\n=== Start ingest from {previous_last_date} ===\n')
        spark = create_session()
        current_last_date = ingest_silver(spark, previous_last_date)

        if current_last_date != previous_last_date and check_date(current_last_date):
            print(f'\n=== Last date: {current_last_date} ===\n')
            list_dates.insert(0, f'{current_last_date}\n')
            if len(list_dates) > 500:
                list_dates.pop()

            # update current_last_date to the tracking file
            with open('./tracking/track_silver', 'w') as fout:
                fout.writelines(list_dates)
                fout.close()
