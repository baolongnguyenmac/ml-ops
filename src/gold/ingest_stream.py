import os

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from gold.ingest_staging import (
    DIM_PAYMENT,
    DIM_RATE_CODE,
    DIM_VENDOR,
    create_session,
    upsert_df,
)
from silver.ingest_silver import SCHEMA as SILVER_SCHEMA
from silver.ingest_silver import TABLE_STREAM as SILVER_TABLE_STREAM

# stream
SCHEMA = 'public'
DIM_LOCATION = 'dim_location'
DIM_STORE_FWD = 'dim_store_fwd'
DIM_DATE = 'dim_date'
TABLE_STREAM = 'fact_trips_stream'
CHECKPOINT = '/home/nblong/Dropbox/Project/ops/ml-ops/spark/checkpoint/gold_stream'
# CHECKPOINT = os.path.join(os.getenv('S3_URI'), 'checkpoint', f'{SCHEMA}_stream')

def read_db_table(spark:SparkSession, db_host:str, db_table:str)->DataFrame:
    return (
        spark.read.format("jdbc")
        .option("driver", "org.postgresql.Driver")
        .option("url", f"jdbc:postgresql://{db_host}:{os.getenv('COM_DB_PORT')}/{os.getenv('COM_GOLD_DB_NAME')}")
        .option("dbtable", db_table)
        .option("user", os.getenv("COM_DB_USER"))
        .option("password", os.getenv("COM_DB_PASSWORD"))
        .load()
    )

def ingest_stream_gold(
    spark:SparkSession,
    source_table:str,
    db_host:str,
    target_table:str,
    checkpoint:str
):
    silver_df = (
        spark.readStream
        .format('delta')
        .option("readChangeFeed", "true")
        .option("startingVersion", "latest") # read from the latest version of table
        .table(source_table)
        .filter(
            F.col("_change_type").isin(["insert", "update_postimage"])
        )
    )

    def _save_stream(batch_df:DataFrame, batch_id:int):
        dim_payment = read_db_table(spark, db_host, f'{SCHEMA}.{DIM_PAYMENT}').cache()
        dim_rate_code = read_db_table(spark, db_host, f'{SCHEMA}.{DIM_RATE_CODE}').cache()
        dim_store_fwd = read_db_table(spark, db_host, f'{SCHEMA}.{DIM_STORE_FWD}').cache()
        dim_vendor = read_db_table(spark, db_host, f'{SCHEMA}.{DIM_VENDOR}').cache()
        pu_dl = read_db_table(spark, db_host, f'{SCHEMA}.{DIM_LOCATION}').cache()
        do_dl = read_db_table(spark, db_host, f'{SCHEMA}.{DIM_LOCATION}').cache()

        # construct fact_trips_tmp to upsert
        fact_trips_tmp = (
            batch_df
            .join(pu_dl, batch_df['pu_location_id']==pu_dl['location_id'], 'left')
            .join(do_dl, batch_df['do_location_id']==do_dl['location_id'], 'left')
            .join(dim_payment, batch_df['payment_type_id']==dim_payment['payment_type_id'], 'left')
            .join(dim_rate_code, batch_df['rate_code_id']==dim_rate_code['rate_code_id'], 'left')
            .join(dim_store_fwd, batch_df['store_and_fwd_id']==dim_store_fwd['store_and_fwd_id'], 'left')
            .join(dim_vendor, batch_df['vendor_id']==dim_vendor['vendor_id'], 'left')
            .select(
                # PK
                batch_df['record_key'],

                # FK
                pu_dl['location_key'].alias('pu_location_key'),
                do_dl['location_key'].alias('do_location_key'),
                dim_payment['dbt_scd_id'].alias('payment_type_key'),
                dim_rate_code['dbt_scd_id'].alias('rate_code_key'),
                dim_store_fwd['store_and_fwd_key'].alias('store_and_fwd_key'),
                dim_vendor['dbt_scd_id'].alias('vendor_key'),
                F.date_format(F.date_trunc("hour", batch_df['pickup_datetime']),  "yyyyMMddHH").cast("int").alias("pickup_date_key"),
                F.date_format(F.date_trunc("hour", batch_df['dropoff_datetime']), "yyyyMMddHH").cast("int").alias("dropoff_date_key"),

                # Since there's no info of location id at stated
                batch_df['pickup_longitude'],
                batch_df['pickup_latitude'],
                batch_df['dropoff_longitude'],
                batch_df['dropoff_latitude'],

                batch_df['pickup_datetime'],
                batch_df['dropoff_datetime'],
                batch_df['passenger_count'],
                batch_df['trip_distance'],
                batch_df['fare_amount'],
                batch_df['extra'],
                batch_df['mta_tax'],
                batch_df['surcharge'],
                batch_df['improvement_surcharge'],
                batch_df['tip_amount'],
                batch_df['tolls_amount'],
                batch_df['total_amount'],
                batch_df['congestion_surcharge'],
                batch_df['airport_fee'],
                batch_df['cbd_congestion_fee'],

                # audit fields
                batch_df['content_hash'],
                batch_df['source_file'],
                batch_df['source_created_datetime'],
                batch_df['bronze_created_datetime'],
                batch_df['year'],
                batch_df['month'],
                batch_df['silver_created_datetime'],
                batch_df['silver_updated_datetime'],
                F.current_timestamp().alias('gold_created_datetime'),
                F.current_timestamp().alias('gold_updated_datetime')
            )
        )

        # upsert fact_trips_tmp to fact_trips_stream
        upsert_df(
            source_df=fact_trips_tmp,
            target_table=target_table,
            db_host=db_host,
            set_cols={"record_key", "pu_location_key", "do_location_key", "payment_type_key", "rate_code_key", "store_and_fwd_key", "vendor_key", "pickup_date_key", "dropoff_date_key", "pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude", "pickup_datetime", "dropoff_datetime", "passenger_count", "trip_distance", "fare_amount", "extra", "mta_tax", "surcharge", "improvement_surcharge", "tip_amount", "tolls_amount", "total_amount", "congestion_surcharge", "airport_fee", "cbd_congestion_fee", "content_hash", "source_file", "source_created_datetime", "bronze_created_datetime", "year", "month", "silver_created_datetime", "silver_updated_datetime", "gold_created_datetime", "gold_updated_datetime"},
            pk='record_key',
            distinct=False,
            audit_cols={"source_file", "source_created_datetime", "bronze_created_datetime", "year", "month", "silver_created_datetime", "gold_created_datetime"}
        )

    # save to postgres
    print(f'\nUpsert to table {target_table}\n')
    query = (
        silver_df.writeStream
        .foreachBatch(_save_stream)
        .trigger(processingTime='30 seconds')
        .option("checkpointLocation", checkpoint)
        .start()
    )
    query.awaitTermination()

def main(lh_host:str='localhost', db_host:str='localhost'):
    spark = create_session(lh_host)

    source_table = f'{SILVER_SCHEMA}.{SILVER_TABLE_STREAM}'
    target_table = f'{SCHEMA}.{TABLE_STREAM}'
    ingest_stream_gold(
        spark=spark,
        source_table=source_table,
        db_host=db_host,
        target_table=target_table,
        checkpoint=CHECKPOINT
    )

if __name__=='__main__':
    # main()
    main(
        lh_host=os.getenv('LH_METASTORE_HOST'),
        db_host=os.getenv('COM_DB_HOST')
    )
