from abc import ABC, abstractmethod
import pandas as pd

from eda.data_loader import DataLoaderFactory

from dotenv import load_dotenv

class DataExplorer(ABC):
    @abstractmethod
    def explorer(self, df:pd.DataFrame):
        pass

class DataTypeExplorer(DataExplorer):
    def explorer(self, df):
        print('== Explore datatype ==\n')
        print(df.info())

class DataSummarization(DataExplorer):
    def explorer(self, df):
        print('== Summarize numeric data ==\n')
        print(df.describe())
        print()
        print('== Summarize categorical data ==\n')
        print(df.describe(include=['O'])) # just don't ask since the doc says so: https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.describe.html

# strategy design pattern: just pass an instance (which you want to work on) to the function
# we can change to factory but it is not rigorous as this method
class DataExplorerStrategy:
    @staticmethod
    def execute_strategy(strategy:DataExplorer, df:pd.DataFrame):
        return strategy.explorer(df)

if __name__=='__main__':
    load_dotenv()

    loader = DataLoaderFactory.get_data_loader('.zip')
    df = loader.load('./db_preparation/archive.zip')
    DataExplorerStrategy.execute_strategy(DataSummarization(), df)
