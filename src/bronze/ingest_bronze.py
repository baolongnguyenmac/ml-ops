from pyspark.sql import SparkSession
import pyspark.sql.functions as F

import os
from dotenv import load_dotenv

from utils.utils import check_date
from bronze.valid_bronze import post_validate_bronze

load_dotenv('.env_file/infra.env')
load_dotenv('.env_file/app.env')

S3_BRONZE_PATH = "s3a://flezi-data/bronze/"
SCHEMA = "bronze"
TABLE  = "nyc_taxi"
SCHEMA_STORE = 'warehouse'

def create_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName('ingest data to bronze zone')
        # jar files supporting read/write postgres, S3, hive (data lake)
        .config(
            'spark.jars',
            """
                ./spark/jar/postgresql-42.7.11.jar,
                ./spark/jar/hadoop-aws-3.4.2.jar,
                ./spark/jar/bundle-2.29.52.jar
            """
        )
        .config("spark.hadoop.fs.s3a.access.key", os.getenv('AWS_ACCESS_KEY_ID'))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv('AWS_SECRET_ACCESS_KEY'))
        # Hive metastore
        .config("spark.hadoop.hive.metastore.uris", f"thrift://{os.getenv('DL_METASTORE_HOST')}:9083")
        .config("spark.sql.catalogImplementation", "hive")
        # create schema using spark (instead of using trino)
        .config("spark.sql.warehouse.dir", f"s3a://flezi-data/{SCHEMA_STORE}/")
        .enableHiveSupport()
        .getOrCreate()
    )

def ingest_bronze(spark: SparkSession, previous_last_date:str) -> str:
    # load data from db
    df = (
        spark.read.format("jdbc")
        .option("driver", "org.postgresql.Driver")
        .option("url", f"jdbc:postgresql://{os.getenv('COM_DB_HOST')}:{os.getenv('COM_DB_PORT')}/{os.getenv('COM_DB_NAME')}")
        .option("dbtable", "public.nyc_taxi")
        .option("user", os.getenv("COM_DB_USER"))
        .option("password", os.getenv("COM_DB_PASSWORD"))
        .load()
    )

    # limit for 3 months, avoid out of mem
    df.createOrReplaceTempView('nyc_taxi')
    df = spark.sql(f'''
        select *
        from nyc_taxi
        where
            (pickup_datetime > '{previous_last_date}')
        order by pickup_datetime asc
        limit {140_000*3};
    ''').cache()  # cache to avoid re-reading from Postgres on each action

    source_count = df.count()
    if source_count == 0:
        return previous_last_date, 0

    # partition by month
    month_partition_df = (
        df.withColumn('year', F.year('pickup_datetime'))
        .withColumn('month', F.month('pickup_datetime'))
    )

    # pre-test
    try:
        partition_count = month_partition_df.count()
        assert source_count == partition_count
    except Exception as e:
        print(f'df.count() != month_partition_df.count() - {e}')
        raise e

    # create schema in trino
    spark.sql(f'create database if not exists {SCHEMA}')
    print(f'\nCreated {SCHEMA}\n')

    # save to data lake
    (
        month_partition_df
        .write
        .partitionBy("year", "month")
        .mode("append")
        .option("path", S3_BRONZE_PATH)
        .saveAsTable(f"{SCHEMA}.{TABLE}")   # auto save metadata to Hive
    )

    print(f"\nTable registered as '{SCHEMA}.{TABLE}' → {S3_BRONZE_PATH}\nInserted {partition_count} rows\n")
    return str(df.select(F.max('pickup_datetime')).collect()[0][0]), source_count

# trigger manually
def main():
    spark = create_session()
    ingest_bronze(spark, '2000-01-01 00:00:00')

# trigger via Airflow schedule
if __name__=='__main__':
    # read the tracking file to get the previous_last_date
    with open('./tracking/track_bronze', 'r') as fi:
        list_dates = fi.readlines()
        previous_last_date = list_dates[0].strip()
        fi.close()

    if check_date(previous_last_date):
        print(f'\n=== Start ingest from {previous_last_date} ===\n')
        spark = create_session()
        current_last_date, ingested_count = ingest_bronze(spark, previous_last_date)

        if current_last_date != previous_last_date and check_date(current_last_date):
            print(f'\n=== Last date: {current_last_date} ===\n')
            list_dates.insert(0, f'{current_last_date}\n')
            if len(list_dates) > 500:
                list_dates.pop()

            # update current_last_date to the tracking file
            with open('./tracking/track_bronze', 'w') as fout:
                fout.writelines(list_dates)
                fout.close()

            # post-test
            post_validate_bronze(spark, previous_last_date, current_last_date, ingested_count)
