from pyspark.sql import SparkSession
import os
import pyspark.sql.functions as F
from dotenv import load_dotenv
import argparse

load_dotenv('.env_file/infra.env')

S3_BRONZE_PATH = "s3a://flezi-data/bronze/"
SCHEMA = "taxi_data"
TABLE  = "nyc_taxi"
SCHEMA_STORE = 'warehouse'

def create_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName('ingest data to bronze zone')
        # read/write S3
        .config("spark.jars.packages", 
                "org.apache.hadoop:hadoop-aws:3.4.2,software.amazon.awssdk:bundle:2.29.38")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv('AWS_ACCESS_KEY_ID'))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv('AWS_SECRET_ACCESS_KEY'))
        # read postgres
        .config('spark.jars', './spark/jar/postgresql-42.7.11.jar')
        # Hive metastore
        .config("spark.hadoop.hive.metastore.uris", f"thrift://{os.getenv('DL_METASTORE_HOST')}:9083")
        .config("spark.sql.catalogImplementation", "hive")
        # create schema using spark (instead of using trino)
        .config("spark.sql.warehouse.dir", f"s3a://flezi-data/{SCHEMA_STORE}/")
        .enableHiveSupport()
        .getOrCreate()
    )

def ingest_bronze(spark: SparkSession, previous_last_date:str):
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

    # filter new records
    df = df.filter(
        df['pickup_datetime'] > previous_last_date
    )

    # partition by month
    month_partition_df = (
        df.withColumn('year', F.year('pickup_datetime'))
        .withColumn('month', F.month('pickup_datetime'))
    )

    # create schema in trino
    spark.sql('create database if not exists taxi_data')

    # save to data lake
    (
        month_partition_df
        .write
        .partitionBy("year", "month")
        .mode("append")
        .option("path", S3_BRONZE_PATH)
        .saveAsTable(f"{SCHEMA}.{TABLE}")   # auto save metadata to Hive
    )

    print(f"Done. Table registered as '{SCHEMA}.{TABLE}' → {S3_BRONZE_PATH}")

    return str(df.select(F.max('pickup_datetime')).collect()[0][0])

# trigger manually
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--lastRun", help="Previous batch job time")
    args = parser.parse_args()

    spark = create_session()
    ingest_bronze(spark, args.lastRun)

# trigger via Airflow schedule
if __name__=='__main__':
    # read the tracking file to get the previous_last_date
    with open('./tracking/track_bronze', 'r') as fi:
        previous_last_date = fi.readline().strip()
        fi.close()

    spark = create_session()
    current_last_date = ingest_bronze(spark, previous_last_date)

    # update current_last_date to the tracking file
    with open('./tracking/track_bronze', 'w') as fout:
        fout.write(current_last_date)
        fout.close()
