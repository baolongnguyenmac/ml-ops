import os

from pyspark.sql import SparkSession


def post_validate_staging(spark:SparkSession, db_host:str, target_table:str, source_count:int):
    print('\n=== Test ===\n')

    staging_count = (
        spark.read.format("jdbc")
        .option("driver", "org.postgresql.Driver")
        .option("url", f"jdbc:postgresql://{db_host}:{os.getenv('COM_DB_PORT')}/{os.getenv('COM_GOLD_DB_NAME')}")
        .option("dbtable", target_table)
        .option("user", os.getenv("COM_DB_USER"))
        .option("password", os.getenv("COM_DB_PASSWORD"))
        .load()
    ).count()

    print(f'Count staging: {staging_count}\nExpected: {source_count}')
    assert staging_count == source_count

    print('\n=== Test pass ===\n')