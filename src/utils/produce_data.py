import os
import time

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from utils.db_connection import PostgresConnection
from utils.schema import LIST_COLS, LIST_DTYPES, SCHEMA_MAPPINGS, SPARK_DTYPES
from utils.utils import parse_args

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

        # lib for modifying postgres, S3
        .config(
            'spark.jars.packages',
            'org.postgresql:postgresql:42.7.11,'
            'org.apache.hadoop:hadoop-aws:3.4.2,'
            'software.amazon.awssdk:bundle:2.29.52,'
        )

        # read/write S3
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
    print(f"\nTable '{table_name}' created successfully.\n")

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

def _insert_batch(df:DataFrame, n_chunk:int, host:str, target_table:str, delay:int=0)->int:
    print(f'\nPush data to table {target_table}\n')

    total_inserted = 0
    if n_chunk != 1:
        chunks = [df.sample(fraction=1/n_chunk, seed=i) for i in range(n_chunk)]
    else:
        chunks = [df]
    for idx, chunk in enumerate(chunks):
        # insert to db
        print(f'\n\tInsert chunk {idx+1}')
        (
            chunk.write
            .format("jdbc")
            .option("url", f"jdbc:postgresql://{host}:5432/{os.getenv('COM_DB_NAME')}")
            .option("dbtable", target_table)
            .option("user", os.getenv('COM_DB_USER'))
            .option("password", os.getenv('COM_DB_PASSWORD'))
            .option("driver", "org.postgresql.Driver")
            .mode("append")
            .save()
        )
        total_inserted += chunk.count()
        time.sleep(delay)

    return total_inserted

def insert_parquet(spark:SparkSession, host:str, file_batch:str, file_stream:str, n:int = 140_000, mode:str='batch'):
    if mode=='batch' or mode=='both':
        df:DataFrame = spark.read.parquet(file_batch)
        f = min(n/df.count(), 1.0)
        df = transform_data(df.sample(fraction=f, seed=42))
        df = df.withColumn('source_file', F.lit(os.path.basename(file_batch)))

    if mode=='stream' or mode=='both':
        df_stream:DataFrame = spark.read.parquet(file_stream)
        f_stream = min(n/df_stream.count(), 1.0)*0.2
        df_stream = transform_data(df_stream.sample(fraction=f_stream, seed=42))
        df_stream = df_stream.withColumn('source_file', F.lit(os.path.basename(file_stream)))

    target_table_batch = 'public.nyc_taxi'
    target_table_stream = 'public.nyc_taxi_stream'

    if mode=='stream':
        n_rows = _insert_batch(df=df_stream, n_chunk=3, host=host, target_table=target_table_stream, delay=3)
        print(f'\nInserted: {n_rows} rows from {file_stream}\n')
    elif mode=='batch':
        n_rows = _insert_batch(df=df, n_chunk=1, host=host, target_table=target_table_batch)
        print(f'\nInserted: {n_rows} rows from {file_batch}\n')
    elif mode=='both':
        n_stream_rows = _insert_batch(df=df_stream, n_chunk=3, host=host, target_table=target_table_stream, delay=3)
        n_batch_rows = _insert_batch(df=df, n_chunk=1, host=host, target_table=target_table_batch)
        print(f'\nInserted: {n_batch_rows} rows from {file_batch} and {n_stream_rows} rows from {file_stream}\n')

def main(host:str='localhost'):
    params = parse_args()
    mode = params['mode']

    pg = PostgresConnection(host)
    with pg as conn:
        create_table(conn, 'public.nyc_taxi')
        create_table(conn, 'public.nyc_taxi_stream')

    spark = create_session()

    # read the tracking file
    with open('./tracking/track_produce_data', 'r') as fin:
        parquet_files = fin.readlines()
        fin.close()

    data_dir = os.path.join(os.getenv('S3_URI'), 'source')

    target_file_batch = parquet_files.pop(0).strip()
    target_file_stream = parquet_files[0].strip()
    print(f'\n===== Process Batch: {target_file_batch}; Stream: {target_file_stream} =====\n')
    file_batch = os.path.join(data_dir, target_file_batch)
    file_stream = os.path.join(data_dir, target_file_stream)
    insert_parquet(spark=spark, host=host, file_batch=file_batch, file_stream=file_stream, mode=mode)

    # write the tracking file
    if mode=='both' or mode=='batch':
        with open('./tracking/track_produce_data', 'w') as fout:
            fout.writelines(parquet_files)
            fout.close()

if __name__=='__main__':
    # main()
    main(host=os.getenv('COM_DB_HOST'))
