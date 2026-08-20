import os
from datetime import datetime, timezone

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from gold.valid_staging import post_validate_staging
from utils.utils import check_date

load_dotenv('.env_file/infra.env')
load_dotenv('.env_file/app.env')

def create_session(lh_host:str) -> SparkSession:
    return (
        SparkSession.builder
        .appName('ingest data to staging of gold')
        .config("spark.driver.memory", "8g")
        .config("spark.executor.memory", "8g")
        .config("spark.sql.session.timeZone", "UTC")
        # delta lake support
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # read/write S3
        .config("spark.hadoop.fs.s3a.access.key", os.getenv('AWS_ACCESS_KEY_ID'))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv('AWS_SECRET_ACCESS_KEY'))
        # read S3, postgres, and delta lake
        .config(
            'spark.jars',
            """
                ./spark/jar/hadoop-aws-3.4.2.jar,
                ./spark/jar/bundle-2.29.52.jar,
                ./spark/jar/delta-spark_4.1_2.13-4.3.0.jar,
                ./spark/jar/delta-kernel-api-4.3.0.jar,
                ./spark/jar/delta-kernel-defaults-4.3.0.jar,
                ./spark/jar/delta-storage-4.3.0.jar,
                ./spark/jar/postgresql-42.7.11.jar,
            """
        )
        # Hive metastore
        .config("spark.hadoop.hive.metastore.uris", f"thrift://{lh_host}:{9083 if lh_host!= 'localhost' else 9084}")
        .config("spark.sql.catalogImplementation", "hive")
        .enableHiveSupport()
        .getOrCreate()
    )

def ingest_staging(spark:SparkSession, watermark:str, db_host:str) -> str:
    # read a batch from silver zone
    batch_start_ts = datetime.now(timezone.utc)
    silver_table = 'silver.nyc_taxi'
    df = spark.read.table(silver_table)
    df.createOrReplaceTempView('nyc_taxi')
    df = spark.sql(f'''
        select * from nyc_taxi
        where silver_updated_datetime <= (
            select max(silver_updated_datetime)
            from (
                select * from nyc_taxi
                where silver_updated_datetime > timestamp '{watermark}'
                order by silver_updated_datetime asc
                limit {140_000*3}
            )
        ) and silver_updated_datetime > timestamp '{watermark}'
        and silver_updated_datetime < '{batch_start_ts!s}'
    ''')

    # overwrite to staging table in postgres of gold
    (
        df.write
        .format("jdbc")
        .option("url", f"jdbc:postgresql://{db_host}:5432/{os.getenv('COM_GOLD_DB_NAME')}")
        .option("dbtable", "nyc_taxi_staging")
        .option("user", os.getenv('COM_DB_USER'))
        .option("password", os.getenv('COM_DB_PASSWORD'))
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .option("truncate", "true") # to keep schema consistent
        .save()
    )

    source_count = df.count()
    print(f"\nTruncate 'gold.public.nyc_taxi_staging'\nInserted {source_count} rows\n")

    return (
        str(df.select(F.max('silver_updated_datetime')).collect()[0][0]),
        source_count
    )

def main(lh_host:str='localhost', db_host:str='localhost'):
    # read the tracking file to get the previous_last_date
    with open('./tracking/track_gold', 'r') as fi:
        list_dates = fi.readlines()
        previous_last_date = list_dates[0].strip()
        fi.close()

    if check_date(previous_last_date):
        print(f'\n=== Start ingest from silver_created_datetime (watermark) > {previous_last_date} ===\n')
        spark = create_session(lh_host)
        current_last_date, source_count = ingest_staging(spark, previous_last_date, db_host)

        if current_last_date != previous_last_date and check_date(current_last_date):
            print(f'\n=== New silver_created_datetime (watermark): {current_last_date} ===\n')

            # post-test
            if source_count != 0:
                post_validate_staging(spark, source_count, db_host=db_host)

            # update current_last_date to the tracking file
            list_dates.insert(0, f'{current_last_date}\n')
            with open('./tracking/track_gold', 'w') as fout:
                fout.writelines(list_dates)
                fout.close()

if __name__=='__main__':
    # main()
    main(lh_host=os.getenv('LH_METASTORE_HOST'), db_host=os.getenv('COM_DB_HOST'))
