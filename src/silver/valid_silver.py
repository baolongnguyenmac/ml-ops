from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def _test_with_version(delta_table:DeltaTable, version:int, source_count:int):
    history_operation = (
        delta_table.history()
        .filter(F.col('version')==version)
        .select('operation', 'operationMetrics').collect()[0]
    )

    operation:str = history_operation[0]
    metrics:dict = history_operation[1]

    if 'merge' in operation.lower():
        inserted = int(metrics['numTargetRowsInserted'])
        updated = int(metrics['numTargetRowsUpdated'])
        expected_source = int(metrics['numSourceRows'])

        print(f'\nInserted: {inserted} rows')
        print(f'Updated: {updated} rows')
        print(f'Expected source: {expected_source} rows')
        print(f'Source counted (from job): {source_count} rows')

        assert expected_source == source_count
        assert updated + inserted <= source_count

    elif 'create' in operation.lower():
        print(f'\nInserted: {metrics["numOutputRows"]} rows')
        print(f'Source counted (from job): {source_count} rows')

        assert int(metrics["numOutputRows"])==source_count

def post_validate_silver(spark:SparkSession, silver_table:str, source_count:int, version:int):
    print('\n=== Test ===\n')

    try:
        delta_table = DeltaTable.forName(spark, silver_table)
        _test_with_version(delta_table, version, source_count)

    except IndexError:
        print(f'\nVersion {version} not existed since there\'s no operation performed\n')

    print('\n=== Test pass ===\n')