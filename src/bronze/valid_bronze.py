import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql import SparkSession


def post_validate_bronze(spark:SparkSession, source_count:int, version:int, audit_timestamp:str):
    """Validate bronze zone after inserting records from data source

    Args:
        spark (SparkSession): SparkSession which is used for ingesting data
        source_count (int): Number of data in df extracted from data source
        version (int): Version of history after ingestion
        audit_timestamp (str): bronze_created_datetime of that batch
    """

    print('\n=== Test ===\n')

    bronze_table = 'bronze.nyc_taxi'
    try:
        delta_table = DeltaTable.forName(spark, bronze_table)
        metrics = (
            delta_table.history()
            .filter(F.col('version')==version)
            .select('operationMetrics').collect()[0][0]
        )
        expected_source = int(metrics['numOutputRows'])

        print(f'''Expected source: {expected_source} rows
Source counted (from job): {source_count} rows
    ''')
        assert expected_source == source_count

    except IndexError as e:
        print(f'\nVersion {version} not existed. Perform manual test\n{e}')
        df_bronze = spark.read.table(bronze_table)
        df_bronze.createOrReplaceTempView('nyc_taxi')

        count_bronze = spark.sql(f'''
            select count(*) from nyc_taxi
            where bronze_created_datetime = '{audit_timestamp}';
        ''').collect()[0][0]

        print(f'Count bronze: {count_bronze}\nExpected: {source_count}\n')
        assert count_bronze == source_count

    print('\n=== Test pass ===\n')
