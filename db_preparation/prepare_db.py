from eda.data_loader import DataLoaderFactory
from eda.data_preprocessor import DataProcessorStrategy, FillNan, MinMaxScaling, OneHotEncoding

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import pandas as pd
import numpy as np

def connect_db():
    DB_PASS = os.environ['POSTGRES_PASSWORD']
    engine = create_engine(f"postgresql+psycopg://postgres:{DB_PASS}@localhost:5432/feast_offline")
    return engine

def main():
    load_dotenv()

    filepath = './db_preparation/archive.zip'
    # loader = DataLoaderFactory().get_data_loader(filepath.split('.')[-1])
    # df = loader.load(filepath)

    # data_processor = DataProcessorStrategy()
    # df = data_processor.execute_strategy(FillNan(), df)
    # df = data_processor.execute_strategy(MinMaxScaling(), df)
    # df = data_processor.execute_strategy(OneHotEncoding(), df)

    # df['id'] = list(range(df.shape[0]))
    # df['event_timestamp'] = pd.Timestamp.now()

    # df_feature = df.drop(['price'], axis=1)
    # df_target = df[['id', 'event_timestamp', 'price']]

    df = pd.read_csv('./db_preparation/superstore.csv', encoding='windows-1252')
    df['created'] = pd.Timestamp.now()

    df['Order Date'] = pd.to_datetime(df['Order Date'], format="%m/%d/%Y") + pd.to_timedelta(np.random.randint(0, 24*60*60, size=len(df)), unit='s')
    df['Ship Date'] = pd.to_datetime(df['Ship Date'], format="%m/%d/%Y") + pd.to_timedelta(np.random.randint(0, 24*60*60, size=len(df)), unit='s')

    df_feature = df.drop(['Row ID', 'Profit'], axis=1)
    df_target = df[['Order Date', 'Order ID', 'Profit', 'created']]

    engine = connect_db()
    df_feature.to_sql('superstore_feature', engine, if_exists='replace', index=False)
    df_target.to_sql('superstore_target', engine, if_exists='replace', index=False)

if __name__=='__main__':
    main()