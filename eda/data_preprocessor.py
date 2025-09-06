from abc import ABC, abstractmethod
import pandas as pd
from sklearn.preprocessing import MinMaxScaler

from eda.data_loader import DataLoaderFactory

from dotenv import load_dotenv

class DataPreprocessor(ABC):
    @abstractmethod
    def transform(self, df:pd.DataFrame) -> pd.DataFrame:
        pass

class FillNan(DataPreprocessor):
    def transform(self, df):
        df = df.drop_duplicates()
        for col in df.columns:
            num_nan = sum(df[col].isnull())
            if num_nan/len(df[col]) <= 0.3:
                if df[col].dtype != 'object':
                    df[col] = df[col].fillna(df[col].mean())
                else:
                    df[col] = df[col].fillna(df[col].mode()[0])
            else:
                df = df.drop(col, axis=1)
        return df

class MinMaxScaling(DataPreprocessor):
    def transform(self, df):
        num_cols = df.select_dtypes(include=["number"]).columns
        obj_cols = df.select_dtypes(exclude=["number"]).columns

        scaler = MinMaxScaler()
        df_scaled_num = pd.DataFrame(
            scaler.fit_transform(df[num_cols]),
            columns=num_cols,
            index=df.index
        )

        return pd.concat([df_scaled_num, df[obj_cols]], axis=1)

class OneHotEncoding(DataPreprocessor):
    def transform(self, df):
        pass

class DataProcessorStrategy:
    @staticmethod
    def execute_strategy(strategy:DataPreprocessor, df:pd.DataFrame):
        return strategy.transform(df)

if __name__=='__main__':
    load_dotenv()

    loader = DataLoaderFactory().get_data_loader('.zip')
    df = loader.load('./tmp/archive.zip')

    df_fillna = DataProcessorStrategy().execute_strategy(FillNan(), df)
    df_transformed = DataProcessorStrategy().execute_strategy(MinMaxScaling(), df_fillna)
    print()