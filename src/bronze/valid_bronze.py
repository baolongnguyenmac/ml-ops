from pyspark.sql import SparkSession, DataFrame
import os

def create_session():
    return (
        SparkSession.builder
        .appName('valid data in bronze zone')
        # read/write S3
        .config("spark.hadoop.fs.s3a.access.key", os.getenv('AWS_ACCESS_KEY_ID'))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv('AWS_SECRET_ACCESS_KEY'))
        # jar files supporting read/write postgres, trino, S3, hive (delta table for lake house)
        .config(
            'spark.jars',
            """
                ./spark/jar/trino-jdbc-480.jar,
                ./spark/jar/postgresql-42.7.11.jar,
                ./spark/jar/hadoop-aws-3.4.2.jar,
                ./spark/jar/bundle-2.29.52.jar
            """
        )
        # Hive metastore
        .config("spark.hadoop.hive.metastore.uris", f"thrift://{os.getenv('LH_METASTORE_HOST')}:9083")
        .config("spark.sql.catalogImplementation", "hive")
        .enableHiveSupport()
        .getOrCreate()
    )

def validate_bronze(start_date:str, end_date:str):
    spark = create_session()
    df_origin = (
        spark.read.format("jdbc")
        .option("driver", "org.postgresql.Driver")
        .option("url", f"jdbc:postgresql://localhost:{os.getenv('COM_DB_PORT')}/{os.getenv('COM_DB_NAME')}")
        .option("dbtable", "public.nyc_taxi")
        .option("user", os.getenv("COM_DB_USER"))
        .option("password", os.getenv("COM_DB_PASSWORD"))
        .load()
    )

    # Connection variables
    trino_url = "jdbc:trino://trino:8080"
    trino_user = "trino"
    trino_password = ""
    target_table = "datalake.taxi_data.nyc_taxi"

    # Read table into a DataFrame
    df_bronze = (
        spark.read
        .option("pushDownPredicate", "false")
        .format("jdbc")
        .option("url", trino_url)
        .option("driver", "io.trino.jdbc.TrinoDriver")
        .option("dbtable", target_table)
        .option("user", trino_user)
        .option("password", trino_password)
        .load()
    )

    count_bronze = df_bronze.filter(
        (df_bronze['pickup_datetime'] <= end_date) &
        (df_bronze['pickup_datetime'] > start_date)
    ).count()

    count_origin = df_origin.filter(
        (df_origin['pickup_datetime'] <= end_date) &
        (df_origin['pickup_datetime'] > start_date)
    ).count()

    print(f'\n=== Test ===\nCount bronze: {count_bronze}\nCount origin: {count_origin}\n')
    assert count_bronze == count_origin
