from abc import ABC, abstractmethod
import os
import pandas as pd
import zipfile

from dotenv import load_dotenv

# can be inherited to handle multiple filetypes
class DataLoader(ABC):
    @abstractmethod
    def load(self, file_path:str) -> pd.DataFrame:
        pass

class CSVLoader(DataLoader):
    def load(self, file_path:str) -> pd.DataFrame:
        if file_path.endswith('.csv'):
            return pd.read_csv(file_path)

        else:
            raise ValueError("Not a *.csv file")

class ZipLoader(DataLoader):
    def load(self, file_path:str) -> list[pd.DataFrame]:
        data_path = os.getenv('DATA_PATH')
        if file_path.endswith('.zip'):
            with zipfile.ZipFile(file_path, 'r') as fi:
                fi.extractall(data_path)

            csv_loader = CSVLoader()
            list_file = os.listdir(data_path)
            if len(list_file) == 1:
                return csv_loader.load(os.path.join(data_path, list_file[0]))
            else:
                raise ValueError("File not found or Multiple files")
        else:
            raise ValueError("Not a *.zip file")

# factory design pattern
class DataLoaderFactory:
    @staticmethod
    def get_data_loader(ext_type:str) -> DataLoader:
        filetypes = {
            # có thể mở rộng tại đây
            'csv': CSVLoader(),
            'zip': ZipLoader()
        }
        return filetypes.get(ext_type, None)

if __name__=='__main__':
    load_dotenv()

    loader = DataLoaderFactory.get_data_loader('.zip')
    df = loader.load('./tmp/archive.zip')
    print(df.info())
