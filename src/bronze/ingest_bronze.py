import os
from datetime import datetime, timezone

import pyspark.sql.functions as F
from dotenv import load_dotenv
from pyspark.sql import SparkSession

from bronze.valid_bronze import post_validate_bronze
from utils.utils import check_date

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
        .config("spark.hadoop.hive.metastore.uris", f"thrift://{os.getenv('LH_METASTORE_HOST')}:9083")
        # .config("spark.hadoop.hive.metastore.uris", "thrift://localhost:9084")
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
        # .option("url", f"jdbc:postgresql://localhost:{os.getenv('COM_DB_PORT')}/{os.getenv('COM_DB_NAME')}")
        .option("dbtable", "public.nyc_taxi")
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
        where source_created_datetime <= (
            select max(source_created_datetime)
            from (
                select * from nyc_taxi
                where source_created_datetime > timestamp '{watermark}'
                order by source_created_datetime asc
                limit {140_000*3}
            )
        ) and source_created_datetime > timestamp '{watermark}'
        and source_created_datetime < '{batch_start_ts!s}'
    ''')

    source_count = df.count()
    if source_count == 0:
        return watermark, 0, None

    # partition ingested time by minute
    bronze_created_datetime = datetime.now(timezone.utc)
    minute_partition_df = (
        df
        .withColumn('bronze_created_datetime', F.lit(bronze_created_datetime).cast('timestamp'))
        .withColumn('year', F.year('bronze_created_datetime'))
        .withColumn('month', F.month('bronze_created_datetime'))
        .withColumn('day', F.month('bronze_created_datetime'))
        .withColumn('hour', F.month('bronze_created_datetime'))
        .withColumn('minute', F.month('bronze_created_datetime')) # since my jobs run every 2 mins
    )

    # create schema in trino
    spark.sql(f'create database if not exists {SCHEMA}')
    print(f'\nCreated {SCHEMA}\n')

    # save to data lake
    (
        minute_partition_df
        .write
        .partitionBy("year", "month", "day", "hour", "minute")
        .mode("append")
        .option("path", S3_BRONZE_PATH)
        .saveAsTable(f"{SCHEMA}.{TABLE}")   # auto save metadata to Hive
    )

    print(f"""
Table registered as '{SCHEMA}.{TABLE}' → {S3_BRONZE_PATH}
\tInserted {source_count} rows\n
""")
    return str(df.select(F.max('source_created_datetime')).collect()[0][0]), source_count, str(bronze_created_datetime)

# trigger manually
def main():
    spark = create_session()
    print(ingest_bronze(spark, '2026-07-20 11:56:56.686864'))

# trigger via Airflow schedule
if __name__=='__main__':
    # main()

    # read the tracking file to get the previous_last_date
    with open('./tracking/track_bronze', 'r') as fi:
        list_dates = fi.readlines()
        previous_watermark = list_dates[0].strip()
        fi.close()

    if check_date(previous_watermark):
        print(f'\n=== Start ingest from: source_created_datetime (watermark) > {previous_watermark} ===\n')
        spark = create_session()
        current_watermark, ingested_count, bronze_created_datetime = ingest_bronze(spark, previous_watermark)

        if current_watermark != previous_watermark and check_date(current_watermark):
            print(f'\n=== New source_created_datetime (watermark) = {current_watermark} ===\n')
            list_dates.insert(0, f'{current_watermark}\n')
            if len(list_dates) > 500:
                list_dates.pop()

            # update current_last_date to the tracking file
            with open('./tracking/track_bronze', 'w') as fout:
                fout.writelines(list_dates)
                fout.close()

            # post-test
            if ingested_count != 0:
                post_validate_bronze(spark, bronze_created_datetime, ingested_count)
