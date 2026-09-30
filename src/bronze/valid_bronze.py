import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql import SparkSession


def post_validate_bronze(spark:SparkSession, table_name:str, source_count:int, version:int):
    """Validate bronze zone after inserting records from data source

    Args:
        spark (SparkSession): SparkSession which is used for ingesting data
        table_name (str): Target table that needs audited
        source_count (int): Number of data in df extracted from data source
        version (int): Version of history after ingestion
        audit_timestamp (str): bronze_created_datetime of that batch
    """

    print('\n=== Test ===\n')

    delta_table = DeltaTable.forName(spark, table_name)
    metrics = (
        delta_table.history()
        .filter(F.col('version')==version)
        .select('operationMetrics').collect()[0][0]
    )
    expected_source = int(metrics['numOutputRows'])

    print(f'Expected source: {expected_source} rows')
    print(f'Source counted (from job): {source_count} rows')
    assert expected_source == source_count

    print('\n=== Test pass ===\n')
