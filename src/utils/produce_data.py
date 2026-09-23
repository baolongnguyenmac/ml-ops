import os

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from utils.db_connection import PostgresConnection
from utils.schema import LIST_COLS, LIST_DTYPES, SCHEMA_MAPPINGS, SPARK_DTYPES

load_dotenv('.env_file/infra.env')
load_dotenv('.env_file/app.env')

def create_session() -> SparkSession:
    return (
        SparkSession
        .builder
        .appName('produce data')
        # support read timestamp with resolution in nano-second (file 2020-01 has its timestamp following this resolution)
        # https://stackoverflow.com/questions/57699926/how-to-fix-illegal-parquet-type-int64-timestamp-micros-error
        .config("spark.sql.legacy.parquet.nanosAsLong", "true")
        # jar files supporting read/write postgres, S3
        .config(
            'spark.jars',
            """
                ./spark/jar/postgresql-42.7.11.jar,
                ./spark/jar/hadoop-aws-3.4.2.jar,
                ./spark/jar/bundle-2.29.52.jar
            """
        )
        .config("spark.hadoop.fs.s3a.access.key", os.getenv('AWS_ACCESS_KEY_ID'))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv('AWS_SECRET_ACCESS_KEY'))
        .getOrCreate()
    )

def create_table(conn, table_name:str):
    """Tạo bảng nyc_taxi nếu chưa tồn tại."""
    query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        vendor_id               TEXT,
        pickup_datetime         TIMESTAMP,
        dropoff_datetime        TIMESTAMP,
        passenger_count         FLOAT,
        trip_distance           FLOAT,
        rate_code_id            FLOAT,
        store_and_fwd_flag      TEXT,
        pickup_longitude        FLOAT,
        pickup_latitude         FLOAT,
        dropoff_longitude       FLOAT,
        dropoff_latitude        FLOAT,
        pu_location_id          TEXT,
        do_location_id          TEXT,
        payment_type            TEXT,
        fare_amount             FLOAT,
        extra                   FLOAT,
        mta_tax                 FLOAT,
        surcharge               FLOAT,
        improvement_surcharge   FLOAT,
        tip_amount              FLOAT,
        tolls_amount            FLOAT,
        total_amount            FLOAT,
        congestion_surcharge    FLOAT,
        airport_fee             FLOAT,
        cbd_congestion_fee      FLOAT,
        source_file             TEXT,
        source_created_datetime TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    );
    """
    with conn.cursor() as cur:
        cur.execute(query)
    conn.commit()
    print(f"Table '{table_name}' created successfully.")

def _resolve_mapping(df_columns: str) -> dict:
    """Tìm schema mapping khớp với columns của file."""
    for schema in SCHEMA_MAPPINGS:
        if schema["columns"] == df_columns:
            return schema["mapping"]
    raise ValueError(f"No mapping found for {df_columns}")

def transform_data(df: DataFrame) -> DataFrame:
    """
    1. Đổi tên cột theo mapping tương ứng.
    2. Thêm cột canonical còn thiếu (fill null).
    3. Đổi kiểu dữ liệu về canonical dtype.
    4. Chỉ giữ lại các cột canonical.
    """
    # 1. Resolve & rename
    mapping = _resolve_mapping('|'.join(df.columns))
    for old_name, new_name in mapping.items():
        df = df.withColumnRenamed(old_name, new_name)

    # 2. Thêm cột còn thiếu
    for col in LIST_COLS:
        if col not in df.columns:
            df = df.withColumn(col, F.lit(None))

    # 3. Cast dtype
    for col, dtype in LIST_DTYPES.items():
        if 'datetime' in col and df.select(col).dtypes[0][1]=='bigint':
            # convert from nano-second to datetime
            # ref: https://stackoverflow.com/questions/72871432/convert-nanosecond-value-into-datetime-using-pyspark-in-databricks
            df = df.withColumn(col, F.to_timestamp(F.from_unixtime(F.col(col)/1000000000), 'yyyy-MM-dd HH:mm:ss'))
        else:
            spark_type = SPARK_DTYPES[dtype]
            df = df.withColumn(col, F.col(col).cast(spark_type))

    # 4. Chỉ giữ các cột canonical
    return df.select(LIST_COLS)

def _insert_batch(df:DataFrame, n_write:int, host:str)->int:
    print('Push data to batch table')

    total_inserted = 0
    for _ in range(n_write):
        # insert to db
        tmp_df = df.sample(1/n_write, seed=42)
        (
            tmp_df
            .write
            .format("jdbc")
            .option("url", f"jdbc:postgresql://{host}:5432/{os.getenv('COM_DB_NAME')}")
            .option("dbtable", "nyc_taxi")
            .option("user", os.getenv('COM_DB_USER'))
            .option("password", os.getenv('COM_DB_PASSWORD'))
            .option("driver", "org.postgresql.Driver")
            .mode("append")
            .save()
        )
        total_inserted += tmp_df.count()

    return total_inserted

def _insert_stream(spark:SparkSession, df:DataFrame, host:str)->int:
    print('Push data to stream table')

    df = (
        df.sample(0.2, seed=42)
        .withColumn('year', F.substring('source_file', 17, 4).cast('int'))
        .withColumn('month', F.substring('source_file', 22, 2).cast('int'))
        .select('*')
        .where((F.col('year')==F.year(F.col('pickup_datetime'))) & (F.col('month')==F.month(F.col('pickup_datetime'))))
    )
    total_rows = df.count()
    batch_size = total_rows//10

    window_spec = Window.orderBy('pickup_datetime')
    df = df.withColumn('index', F.row_number().over(window_spec) - 1)
    df.createOrReplaceTempView('nyc_taxi_stream')

    for i in range(0, total_rows, batch_size):
        start_index = i
        end_index = min(i+batch_size, total_rows)
        print(f'\tProcess from {start_index} to {end_index}')

        tmp_df = spark.sql(f'''
            select *
            from nyc_taxi_stream
            where (nyc_taxi_stream.index >= {start_index}) and (nyc_taxi_stream.index < {end_index})
        ''')
        (
            tmp_df.drop('index', 'year', 'month')
            .write
            .format("jdbc")
            .option("url", f"jdbc:postgresql://{host}:5432/{os.getenv('COM_DB_NAME')}")
            .option("dbtable", "nyc_taxi_stream")
            .option("user", os.getenv('COM_DB_USER'))
            .option("password", os.getenv('COM_DB_PASSWORD'))
            .option("driver", "org.postgresql.Driver")
            .mode("append")
            .save()
        )
    return total_rows

def insert_parquet(spark:SparkSession, host:str, filepath:str, n:int = 140_000, mode:str='batch'):
    # read parquet
    df:DataFrame = (spark.read.parquet(filepath))

    # sample n rows
    fraction = min(n/df.count(), 1.0)
    df = transform_data(df.sample(fraction=fraction, seed=42))
    df = df.withColumn('source_file', F.lit(os.path.basename(filepath)))

    # insert data into postgres
    n_write = 10
    if mode=='stream':
        return _insert_stream(spark, df, host)
    elif mode=='batch':
        # write 10 times to db
        return _insert_batch(df, n_write, host)
    elif mode=='both':
        return _insert_stream(spark, df, host) + _insert_batch(df, n_write, host)

def main(host:str='localhost', mode:str='stream'):
    pg = PostgresConnection(host)
    with pg as conn:
        # create_table(conn, 'public.nyc_taxi')
        create_table(conn, 'public.nyc_taxi_stream')

    spark = create_session()

    # read the tracking file
    with open('./tracking/track_produce_data', 'r') as fin:
        parquet_files = fin.readlines()
        fin.close()

    data_dir = os.path.join(os.getenv('S3_URI'), 'source')

    target_file = parquet_files.pop(0).strip()
    print(f'\n===== Process {target_file} =====\n')
    filepath = os.path.join(data_dir, target_file)
    n_new_rows = insert_parquet(spark=spark, host=host, filepath=filepath, mode=mode)
    print(f'\nInserted: {n_new_rows} rows from {target_file}\n')

    # write the rest back the file
    if mode=='both':
        with open('./tracking/track_produce_data', 'w') as fout:
            fout.writelines(parquet_files)
            fout.close()

if __name__=='__main__':
    # main()
    main(host=os.getenv('COM_DB_HOST'), mode='both')
