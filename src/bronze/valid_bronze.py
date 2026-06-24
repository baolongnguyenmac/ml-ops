from pyspark.sql import SparkSession

# from dotenv import load_dotenv

# load_dotenv('.env_file/infra.env')
# load_dotenv('.env_file/app.env')

def post_validate_bronze(spark:SparkSession, start_date:str, end_date:str, expected_count:int):
    # Read table from bronze into a DataFrame
    source_table = "bronze.nyc_taxi"
    df_bronze = spark.read.table(source_table)

    count_bronze = df_bronze.filter(
        (df_bronze['pickup_datetime'] <= end_date) &
        (df_bronze['pickup_datetime'] > start_date)
    ).count()

    print(f'Count bronze: {count_bronze}\nExpected (from cached source): {expected_count}\n')
    assert count_bronze == expected_count
    print('\n=== Test pass ===\n')

# if __name__=='__main__':
#     spark = create_session()
#     post_validate_bronze(spark, '2000-01-01 00:00:00', '2009-04-30 23:59:02')
