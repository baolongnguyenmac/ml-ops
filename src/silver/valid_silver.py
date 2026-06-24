from pyspark.sql import SparkSession

def post_valid_silver(spark:SparkSession, start_date:str, end_date:str, expected_count:int):
    silver_table = 'silver.nyc_taxi'
    df_silver = spark.read.table(silver_table)

    count_silver = df_silver.filter(
        (df_silver['pickup_datetime'] <= end_date) &
        (df_silver['pickup_datetime'] > start_date)
    ).count()

    print(f'Count silver: {count_silver}\nExpected (from cached source): {expected_count}\n')
    assert count_silver == expected_count
    print('\n=== Test pass ===\n')