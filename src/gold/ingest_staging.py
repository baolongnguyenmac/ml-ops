import os
from datetime import datetime, timezone

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from gold.valid_staging import post_validate_staging
from silver.ingest_silver import SCHEMA as SILVER_SCHEMA
from silver.ingest_silver import TABLE_BATCH as SILVER_TABLE_BATCH
from utils.db_connection import PostgresConnection
from utils.utils import check_date

load_dotenv('.env_file/infra.env')
load_dotenv('.env_file/app.env')

SCHEMA = 'staging'
FACT_SOURCE  = "nyc_taxi"
DIM_VENDOR = 'dim_vendor'
DIM_PAYMENT = 'dim_payment_type'
DIM_RATE_CODE = 'dim_rate_code'

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
            'spark.jars.packages',
            'org.apache.hadoop:hadoop-aws:3.4.2,'
            'software.amazon.awssdk:bundle:2.29.52,'
            'io.delta:delta-spark_4.1_2.13:4.3.0,'
            'org.postgresql:postgresql:42.7.11,'
        )
        # Hive metastore
        .config("spark.hadoop.hive.metastore.uris", f"thrift://{lh_host}:{9083 if lh_host!= 'localhost' else 9084}")
        .config("spark.sql.catalogImplementation", "hive")
        .enableHiveSupport()
        .getOrCreate()
    )

def _save_batch(df:DataFrame, db_host:str, target_table:str)->int:
    (
        df.write
        .format("jdbc")
        .option("url", f"jdbc:postgresql://{db_host}:{os.getenv('COM_DB_PORT')}/{os.getenv('COM_GOLD_DB_NAME')}")
        .option("dbtable", target_table)
        .option("user", os.getenv('COM_DB_USER'))
        .option("password", os.getenv('COM_DB_PASSWORD'))
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .option("truncate", "true") # to keep schema consistent
        .save()
    )
    source_count = df.count()
    print(f"\nOverwrite table '{target_table}'\nInserted {source_count} rows\n")
    return source_count

def upsert_staging_dim(source_df:DataFrame, target_table:str, db_host:str, set_cols:set[str], pk:str):
    (
        source_df.select(*set_cols).distinct().dropna(subset=[pk])
        .write
        .format("jdbc")
        .option("url", f"jdbc:postgresql://{db_host}:{os.getenv('COM_DB_PORT')}/{os.getenv('COM_GOLD_DB_NAME')}")
        .option("dbtable", f'{target_table}_tmp')
        .option("user", os.getenv('COM_DB_USER'))
        .option("password", os.getenv('COM_DB_PASSWORD'))
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )

    updated_cols = set_cols - {pk}
    updated_set = [f'{col} = EXCLUDED.{col}' for col in updated_cols]

    query = f'''
        insert into {target_table} ({",".join(set_cols)})
        select {",".join(set_cols)}
        from {target_table}_tmp
        on conflict ({pk}) do update
            set {", ".join(updated_set)}
    '''

    pg = PostgresConnection(host=db_host, dbname=os.getenv('COM_GOLD_DB_NAME'))
    with pg as conn, conn.cursor() as cur:
        cur.execute(query)
        cur.execute(f'drop table {target_table}_tmp')
        conn.commit()

def ingest_staging(
    spark:SparkSession,
    watermark:str,
    db_host:str,
    source_table:str,
    target_table:str
) -> str:
    # read a batch from silver zone
    batch_start_ts = datetime.now(timezone.utc)
    df = spark.read.table(source_table)
    df.createOrReplaceTempView('nyc_taxi')
    df = spark.sql(f'''
        select * from nyc_taxi
        where silver_updated_datetime >= timestamp '{watermark}'
        and silver_updated_datetime < '{batch_start_ts!s}'
        limit {140_000*3}
    ''')

    # create staging schema
    pg = PostgresConnection(host=db_host, dbname=os.getenv('COM_GOLD_DB_NAME'))
    with pg as conn, conn.cursor() as cur:
        cur.execute(f'create schema if not exists {SCHEMA}')
        conn.commit()
    print(f'\nCreated {SCHEMA}\n')

    if not df.isEmpty():
        # overwrite staging.nyc_taxi (used for fact_trips)
        source_count = _save_batch(df, db_host, target_table)
        new_watermark = str(df.select(F.max('silver_updated_datetime')).collect()[0][0])

        # upsert dim_vendor, dim_payment_type, dim_rate_code
        upsert_staging_dim(df, f'{SCHEMA}.{DIM_PAYMENT}', db_host, {'payment_type_id', 'payment_type'}, 'payment_type_id')
        upsert_staging_dim(df, f'{SCHEMA}.{DIM_VENDOR}', db_host, {'vendor_id', 'vendor_name'}, 'vendor_id')
        upsert_staging_dim(df, f'{SCHEMA}.{DIM_RATE_CODE}', db_host, {'rate_code_id', 'rate_code'}, 'rate_code_id')
    else:
        source_count = 0
        new_watermark = watermark

    return (
        new_watermark,
        source_count
    )

def main(lh_host:str='localhost', db_host:str='localhost'):
    # update lại logic này, chia rõ dim và fact tại staging

    # read the tracking file to get the previous_last_date
    with open('./tracking/track_gold', 'r') as fi:
        list_dates = fi.readlines()
        previous_last_date = list_dates[0].strip()
        fi.close()

    if check_date(previous_last_date):
        print(f'\n=== Start ingest from silver_updated_datetime (watermark) >= {previous_last_date} ===\n')
        spark = create_session(lh_host)
        source_table = f'{SILVER_SCHEMA}.{SILVER_TABLE_BATCH}'
        target_table = f'{SCHEMA}.{FACT_SOURCE}'
        current_last_date, source_count = ingest_staging(
            spark=spark,
            watermark=previous_last_date,
            db_host=db_host,
            source_table=source_table,
            target_table=target_table
        )

        if current_last_date != previous_last_date and check_date(current_last_date):
            print(f'\n=== New silver_updated_datetime (watermark): {current_last_date} ===\n')

            # post-test
            if source_count != 0:
                post_validate_staging(spark=spark, db_host=db_host, target_table=target_table, source_count=source_count)

            # update current_last_date to the tracking file
            list_dates.insert(0, f'{current_last_date}\n')
            with open('./tracking/track_gold', 'w') as fout:
                fout.writelines(list_dates)
                fout.close()

if __name__=='__main__':
    # main()
    main(lh_host=os.getenv('LH_METASTORE_HOST'), db_host=os.getenv('COM_DB_HOST'))
