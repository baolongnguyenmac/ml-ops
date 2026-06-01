from pyspark.sql import SparkSession
import os
import pyspark.sql.functions as F
from dotenv import load_dotenv

load_dotenv()

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
        # Hive metastore on localhost
        .config("spark.hadoop.hive.metastore.uris", "thrift://localhost:9083")
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.sql.hive.metastore.version", "3.1.3")
        .config("spark.sql.hive.metastore.jars", "maven")
        # create schema using spark (instead of using trino)
        .config("spark.sql.warehouse.dir", f"s3a://flezi-data/{SCHEMA_STORE}/")
        .enableHiveSupport()
        .getOrCreate()
    )

def ingest_bronze(spark: SparkSession, last_date:str):
    # load data from db
    df = (
        spark.read.format("jdbc")
        .option("driver", "org.postgresql.Driver")
        .option("url", f"jdbc:postgresql://localhost:{os.getenv('COM_DB_PORT')}/{os.getenv('COM_DB_NAME')}")
        .option("dbtable", "public.nyc_taxi")
        .option("user", os.getenv("COM_DB_USER"))
        .option("password", os.getenv("COM_DB_PASSWORD"))
        .load()
    )

    # filter new records
    df = df.filter(
        df['pickup_datetime'] > last_date
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
