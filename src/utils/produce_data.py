from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
import s3fs
import time
import os

from utils.db_connection import PostgresConnection
from utils.schema import LIST_COLS, LIST_DTYPES, SCHEMA_MAPPINGS, SPARK_DTYPES

from dotenv import load_dotenv
load_dotenv('.env_file/infra.env')

def create_session() -> SparkSession:
    return (
        SparkSession
        .builder
        .appName('produce data')
        .config('spark.jars', './spark/jar/postgresql-42.7.11.jar')
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.2,software.amazon.awssdk:bundle:2.29.38")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv('AWS_ACCESS_KEY_ID'))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv('AWS_SECRET_ACCESS_KEY'))
        .getOrCreate()
    )

def create_table(conn):
    """Tạo bảng nyc_taxi nếu chưa tồn tại."""
    query = """
    CREATE TABLE IF NOT EXISTS public.nyc_taxi (
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
        cbd_congestion_fee      FLOAT
    );
    """
    with conn.cursor() as cur:
        cur.execute(query)
    conn.commit()
    print("Table 'nyc_taxi' created successfully.")

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
        spark_type = SPARK_DTYPES[dtype]
        df = df.withColumn(col, F.col(col).cast(spark_type))

    # 4. Chỉ giữ các cột canonical
    return df.select(LIST_COLS)

def insert_parquet(spark: SparkSession, filepath: str, n: int = 140_000, batch_size: int = 17_000, interval: float = 1.0, chunk_size:int=120):
    # read parquet
    df:DataFrame = (
        spark.read.parquet(filepath)
        # .repartition(os.path.getsize(filepath)/(1024**2)//chunk_size)
    )

    # sample n rows
    fraction = min(n/df.count(), 1)
    df = transform_data(df.sample(fraction=fraction, seed=42))

    # sort by pickup_datetime
    window_spec = Window.orderBy('pickup_datetime')
    df = df.withColumn('index', F.row_number().over(window_spec) - 1)
    df.createOrReplaceTempView('nyc_taxi')

    # insert to db
    total_rows = df.count()
    for i in range(0, total_rows, batch_size):
        start_index = i
        end_index = min(i+batch_size, total_rows)
        print(f'\tProcess from {start_index} to {end_index}')

        tmp_df = spark.sql(f'''
            select *
            from nyc_taxi
            where (nyc_taxi.index >= {start_index}) and (nyc_taxi.index < {end_index})
        ''')
        tmp_df.drop('index').write \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://company_db:5432/{os.getenv('COM_DB_NAME')}") \
            .option("dbtable", "nyc_taxi") \
            .option("user", os.getenv('COM_DB_USER')) \
            .option("password", os.getenv('COM_DB_PASSWORD')) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        time.sleep(interval)

    print(f"Done! Inserted {total_rows} rows from {filepath}")
    return total_rows

# produce manually
def main():
    pg = PostgresConnection(port=5432)
    fs = s3fs.S3FileSystem()
    n_rows = 0

    data_dir = 's3://flezi-data/nyc-taxi/'
    list_files = fs.listdir(data_dir)

    with pg as conn:
        create_table(conn)

    spark = create_session()

    # insert parquet into db
    for idx, item in enumerate(list_files):
        filename = item['Key'].split('/')[-1]
        if item['Key'].endswith('.parquet'):
            print(f'\n===== {idx}/{len(list_files)}. Process {filename} =====\n')
            filepath = os.path.join(data_dir, filename).replace('s3', 's3a')
            n_new_rows = insert_parquet(spark, filepath)
            n_rows += n_new_rows
            print(f'\nTotal inserted: {n_rows}\n')
        time.sleep(10)

# produce by Airflow schedule
if __name__=='__main__':
    # setup environment
    pg = PostgresConnection(port=5432, host='company_db')
    fs = s3fs.S3FileSystem()

    data_dir = 's3://flezi-data/nyc-taxi/'
    list_files = fs.listdir(data_dir)

    with pg as conn:
        create_table(conn)

    spark = create_session()

    # read the tracking file
    with open('./tracking/track_produce_data', 'r') as fin:
        parquet_files = fin.readlines()
        fin.close()

    target_file = parquet_files.pop(0).strip()
    print(f'\n===== Process {target_file} =====\n')
    filepath = os.path.join(data_dir, target_file).replace('s3', 's3a')
    n_new_rows = insert_parquet(spark, filepath)
    print(f'\nInserted: {n_new_rows} rows from {target_file}\n')

    # write the rest back the file
    with open('./tracking/track_produce_data', 'w') as fout:
        fout.writelines(parquet_files)
        fout.close()
