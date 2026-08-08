# Qwen3.6-27B

import os
from datetime import timezone

import holidays
import pandas as pd
import psycopg2.extras
from dotenv import load_dotenv

from utils.db_connection import PostgresConnection

load_dotenv('.env_file/infra.env')
load_dotenv('.env_file/app.env')

def _generate_dim_date(start: str, end: str) -> pd.DataFrame:
    dt_hour = pd.date_range(start=start, end=end, freq="h")

    # Generate US holidays cho toàn bộ range năm
    start_year = pd.Timestamp(start).year
    end_year   = pd.Timestamp(end).year

    # state="NY" để có thêm holiday của New York
    us_holidays = holidays.US(years=range(start_year, end_year + 1), state="NY")

    holiday_dates  = set(us_holidays.keys())
    holiday_names  = {date: name for date, name in us_holidays.items()}

    df = pd.DataFrame({
        "datetime_key":  dt_hour.strftime("%Y%m%d%H").astype(int),
        "full_datetime": dt_hour,
        "hour":          dt_hour.hour,
        "day":           dt_hour.day,
        "day_of_week":   dt_hour.dayofweek,
        "day_name":      dt_hour.day_name(),
        "month":         dt_hour.month,
        "month_name":    dt_hour.month_name(),
        "quarter":       dt_hour.quarter,
        "year":          dt_hour.year,
        "is_weekend":    dt_hour.dayofweek >= 5,
        "is_rush_hour":  dt_hour.hour.isin(range(7, 10)) | dt_hour.hour.isin(range(17, 20)),

        # Holiday columns
        "is_holiday":    dt_hour.normalize().map(lambda d: d.date() in holiday_dates),
        "holiday_name":  dt_hour.normalize().map(lambda d: holiday_names.get(d.date(), None)),
    })

    return df

def write_to_postgres(df: pd.DataFrame, table_name: str = "dim_date") -> None:
    """Ghi DataFrame vào bảng PostgreSQL (mode: truncate then insert)."""
    cols = ", ".join(df.columns)
    placeholders = ", ".join(["%s"] * len(df.columns))
    insert_sql = f"INSERT INTO public.{table_name} ({cols}) VALUES ({placeholders})"

    values = [tuple(row) for _, row in df.iterrows()]

    with PostgresConnection(host=os.getenv('COM_DB_HOST'), dbname=os.getenv('COM_GOLD_DB_NAME')) as conn, conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE public.dim_date CASCADE")
        psycopg2.extras.execute_batch(cur, insert_sql, values, page_size=5000)
        conn.commit()

    print(f"Inserted {len(df)} rows into public.{table_name}")

def create_gold_schema(filepath:str='./sql/gold_schema.sql'):
    with open(filepath, 'r') as fi:
        query = ''.join(fi.readlines())

    with PostgresConnection(host=os.getenv('COM_DB_HOST'), dbname=os.getenv('COM_GOLD_DB_NAME')) as conn, conn.cursor() as cur:
        cur.execute(query)
        conn.commit()

    print('Created schema for Gold zone')

def create_dim_date(start:str, end:str):
    # Check if existing dim_date covers the required range
    need_refresh = False
    with PostgresConnection(host=os.getenv('COM_DB_HOST'), dbname=os.getenv('COM_GOLD_DB_NAME')) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT MIN(full_datetime)::text, MAX(full_datetime)::text FROM public.dim_date"
        )
        output = cur.fetchone()

    if not output or output[0] is None:
        need_refresh = True
    else:
        min_dt = pd.Timestamp(output[0], tzinfo=timezone.utc).floor("h")
        max_dt = pd.Timestamp(output[1], tzinfo=timezone.utc).floor("h")
        if min_dt < pd.Timestamp(start) or max_dt > pd.Timestamp(end):
            need_refresh = True

    if need_refresh:
        df = _generate_dim_date(start, end)
        write_to_postgres(df)
        print(f'Generated data for dim_date in range of [{start}, {end}]')
    else:
        print(f"dim_date already covers [{start}, {end}]. Skipping.")

if __name__=='__main__':
    # create gold schema
    create_gold_schema()

    # generate data for dim_date
    start='2009-01-01 00:00:00+00'
    end='2025-12-31 23:59:59+00'
    create_dim_date(start, end)
