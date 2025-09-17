from feast import FeatureStore
import pandas as pd
from datetime import datetime, timedelta
import subprocess

class MyFeatureStore:
    def __init__(self) -> None:
        self.store:FeatureStore = FeatureStore(repo_path='./feature_store')

    def fetch_historical_feature(self, entity_df:pd.DataFrame, features:list[str]=None) -> pd.DataFrame:
        if features is None:
            return self.store.get_historical_features(
                entity_df,
                features=self.store.get_feature_service('driver_fs')
            ).to_df()
        return self.store.get_historical_features(entity_df, features=features).to_df()

    def _sync_to_online(self, fv:str='driver_fv'):
        end_date = datetime.now().isoformat()
        
        # testing
        start_date = end_date - timedelta(days=1)
        subprocess.run(['feast', 'materialize', start_date, end_date, '--views', fv])

        # # production
        # subprocess.run(['feast', 'materialize-incremental', end_date, '--views', fv])

    def fetch_online_feature(self, entity_row:list[dict], features:list[str]) -> pd.DataFrame:
        self._sync_to_online()
        if features is None:
            return self.store.get_online_features(
                features=self.store.get_feature_service('driver_fs'),
                entity_row=entity_row
            ).to_df()
        return self.store.get_online_features(features=features, entity_row=entity_row).to_df()
