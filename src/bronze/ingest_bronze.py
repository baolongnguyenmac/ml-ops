from pyspark.sql import SparkSession
import pyspark.sql.functions as F

import os
from dotenv import load_dotenv
from datetime import datetime, timezone

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
        .config("spark.driver.memory", "8g")
        .config("spark.executor.memory", "8g")
        .config("spark.sql.session.timeZone", "UTC")
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

def ingest_bronze(spark: SparkSession, watermark:str) -> str:
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
        .option("url", f"jdbc:postgresql://{os.getenv('COM_DB_HOST')}:{os.getenv('COM_DB_PORT')}/{os.getenv('COM_DB_NAME')}")
        .option("dbtable", "public.nyc_taxi")
        .option("user", os.getenv("COM_DB_USER"))
        .option("password", os.getenv("COM_DB_PASSWORD"))
        .load()
    )

    # limit for 3 months, avoid out of mem
    df.createOrReplaceTempView('nyc_taxi')
    df = spark.sql(f'''
        select * from nyc_taxi
        where source_created_datetime <= (
            select max(source_created_datetime)
            from (
                select * from nyc_taxi
                where source_created_datetime > timestamp '{watermark}'
                order by source_created_datetime asc
                limit {140_000*3}
            )
        ) and source_created_datetime > timestamp '{watermark}';
    ''').cache()  # cache to avoid re-reading from Postgres on each action

    source_count = df.count()
    if source_count == 0:
        return watermark, 0, None

    # partition by month
    month_partition_df = (
        df
        .withColumn('year', F.year('pickup_datetime'))
        .withColumn('month', F.month('pickup_datetime'))
    )

    # create schema in trino
    spark.sql(f'create database if not exists {SCHEMA}')
    print(f'\nCreated {SCHEMA}\n')

    # save to data lake
    batch_timestamp = datetime.now(timezone.utc)
    (
        month_partition_df.withColumn('bronze_created_datetime', F.lit(batch_timestamp).cast('timestamp'))
        .write
        .partitionBy("year", "month")
        .mode("append")
        .option("path", S3_BRONZE_PATH)
        .saveAsTable(f"{SCHEMA}.{TABLE}")   # auto save metadata to Hive
    )

    print(f"""
Table registered as '{SCHEMA}.{TABLE}' → {S3_BRONZE_PATH}
\tInserted {source_count} rows\n
""")
    return str(df.select(F.max('source_created_datetime')).collect()[0][0]), source_count, str(batch_timestamp).split('+')[0]

# trigger manually
def main():
    spark = create_session()
    print(ingest_bronze(spark, '2000-01-01 00:00:00.0'))

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
        current_last_date, ingested_count, bronze_created_datetime = ingest_bronze(spark, previous_last_date)

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
            if ingested_count != 0:
                post_validate_bronze(spark, bronze_created_datetime, ingested_count)
