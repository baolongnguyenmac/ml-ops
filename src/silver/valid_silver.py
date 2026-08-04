from pyspark.sql import SparkSession


def post_valid_silver(spark:SparkSession, audit_timestamp:str, expected_count:int):
    silver_table = 'silver.nyc_taxi'
    df_silver = spark.read.table(silver_table)
    df_silver.createOrReplaceTempView('nyc_taxi')

    count_silver = spark.sql(f'''
        select count(*) from nyc_taxi
        where silver_updated_datetime = '{audit_timestamp}';
    ''').collect()[0][0]

    print(f'Count silver: {count_silver}\nExpected: {expected_count}\n')
    assert count_silver == expected_count
    print('\n=== Test pass ===\n')