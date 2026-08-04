from pyspark.sql import SparkSession

# from dotenv import load_dotenv

# load_dotenv('.env_file/infra.env')
# load_dotenv('.env_file/app.env')

def post_validate_bronze(spark:SparkSession, audit_timestamp:str, expected_count:int):
    # Read table from bronze into a DataFrame
    source_table = "bronze.nyc_taxi"
    df_bronze = spark.read.table(source_table)
    df_bronze.createOrReplaceTempView('nyc_taxi')

    count_bronze = spark.sql(f'''
        select count(*) from nyc_taxi
        where bronze_created_datetime = '{audit_timestamp}';
    ''').collect()[0][0]

    print(f'Count bronze: {count_bronze}\nExpected: {expected_count}\n')
    assert count_bronze == expected_count
    print('\n=== Test pass ===\n')

# if __name__=='__main__':
#     spark = create_session()
#     post_validate_bronze(spark, '2000-01-01 00:00:00', '2009-04-30 23:59:02')
