from eda.data_loader import DataLoaderFactory
from eda.data_preprocessor import DataProcessorStrategy, FillNan, MinMaxScaling, OneHotEncoding

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import pandas as pd

def connect_db():
    # PASS = os.environ['POSTGRES_PASSWORD']
    engine = create_engine("postgresql+psycopg2://postgres:Long123ohio@localhost:5432/feast_offline")
    return engine

def main():
    load_dotenv()

    filepath = './tmp/archive.zip'
    loader = DataLoaderFactory().get_data_loader(filepath.split('.')[-1])
    df = loader.load(filepath)

    data_processor = DataProcessorStrategy()
    df = data_processor.execute_strategy(FillNan(), df)
    df = data_processor.execute_strategy(MinMaxScaling(), df)
    df = data_processor.execute_strategy(OneHotEncoding(), df)

    df['id'] = list(range(df.shape[0]))
    df['event_timestamp'] = pd.Timestamp.now()

    df_feature = df.drop(['price'], axis=1)
    df_target = df[['id', 'event_timestamp', 'price']]

    engine = connect_db()
    df_feature.to_sql('house_features', engine, if_exists='replace', index=False)
    df_target.to_sql('house_target', engine, if_exists='replace', index=False)

if __name__=='__main__':
    main()