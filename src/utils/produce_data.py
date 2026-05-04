from utils.db_connection import PostgresConnection
import numpy as np
import pandas as pd
import s3fs
import time
import os

from utils.schema import LIST_COLS, LIST_DTYPES, SCHEMA_MAPPINGS

from dotenv import load_dotenv
load_dotenv()

def create_table(conn):
    """Tạo bảng nyc_taxi nếu chưa tồn tại."""
    query = """
    CREATE TABLE IF NOT EXISTS nyc_taxi (
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

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    1. Đổi tên cột theo mapping tương ứng.
    2. Thêm cột canonical còn thiếu (fill NaN).
    3. Đổi kiểu dữ liệu về canonical dtype.
    4. Thay NaN → None để insert NULL vào Postgres.
    """
    # 1. Resolve & rename
    mapping = _resolve_mapping('|'.join(df.columns))
    df.rename(columns=mapping, inplace=True)

    # 2. Thêm cột còn thiếu
    for col in LIST_COLS:
        if col not in df.columns:
            df[col] = np.nan

    # Giữ đúng thứ tự canonical
    df = df[LIST_COLS]

    # 3. Cast dtype
    for col, dtype in LIST_DTYPES.items():
        if dtype == "datetime64[ns]":
            df[col] = pd.to_datetime(df[col], errors="coerce")
        elif dtype == "float64":
            df[col] = pd.to_numeric(df[col], errors="coerce")
        else:  # object
            df[col] = df[col].astype("object")

    # 4. NaN → None
    return df.astype(object).where(df.notna(), None)[LIST_COLS]

def insert_parquet(conn, filepath: str, n: int = 1, batch_size: int = 17_000, interval: float = 1.0):
    """Đọc file parquet, lấy n sample cách đều, rồi insert theo batch."""
    df = pd.read_parquet(filepath)

    # Lấy n sample cách đều
    indices = np.linspace(0, len(df) - 1, num=n, dtype=int)
    df = df.iloc[indices].reset_index(drop=True)

    df = transform_data(df)

    placeholders = ', '.join(['%s'] * len(LIST_COLS))
    insert_query = f"INSERT INTO nyc_taxi ({', '.join(LIST_COLS)}) VALUES ({placeholders})"

    total_rows = len(df)
    inserted = 0

    with conn.cursor() as cur:
        while inserted < total_rows:
            batch = df.iloc[inserted:inserted + batch_size]
            values = [tuple(row) for row in batch.itertuples(index=False, name=None)]
            cur.executemany(insert_query, values)
            conn.commit()
            inserted += len(batch)
            print(f"Inserted {inserted}/{total_rows} rows")
            if inserted < total_rows:
                time.sleep(interval)

    print(f"Done! Inserted {total_rows} rows from {filepath}")

if __name__=='__main__':
    pg = PostgresConnection(port=5432)
    fs = s3fs.S3FileSystem()

    data_dir = 's3://flezi-agent/nyc-taxi/'
    list_files = fs.listdir(data_dir)

    with pg as conn:
        create_table(conn)
        for idx, item in enumerate(list_files):
            filename = item['Key'].split('/')[-1]
            if item['Key'].endswith('.parquet'):
                filepath = os.path.join(data_dir, filename)
                insert_parquet(conn, filepath=filepath, n=140_000)
            time.sleep(10)
