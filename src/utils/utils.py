import argparse
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import SparkSession


def check_date(date_str:str) -> bool:
    # check if the string is valid by converting to time
    try:
        datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S.%f%z")
        return True
    except ValueError:
        try:
            datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S.%f").astimezone(timezone.utc)
            return True
        except ValueError as e:
            raise f'[check_date] Expect string in timestamp but receive {date_str} - {e}'

def _get_table_version(spark:SparkSession, table_name:str):
    """Get the version of table on Delta lake

    Args:
        spark (SparkSession): Spark Session
        table_name (str): Table name on Delta lake

    Returns:
        int: The version of table. -1 if the table doesn't exist
    """
    # get version before insert
    if spark.catalog.tableExists(table_name):
        delta_table = DeltaTable.forName(spark, table_name)
        version = (
            delta_table.history(limit=1)
            .select('version').collect()[0][0]
        )
    else:
        version = -1
    return version

def parse_args():
    parser = argparse.ArgumentParser()
    # option `both` is only used for `produce_data.py`
    parser.add_argument('-m', '--mode', type=str, choices=['batch', 'stream', 'both'], default='batch', required=True)

    return vars(parser.parse_args())
