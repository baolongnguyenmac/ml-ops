from delta.tables import DeltaTable
from pyspark.sql import SparkSession


def post_valid_silver(spark:SparkSession, source_count:int):
    print('\n=== Test ===\n')
    silver_table = 'silver.nyc_taxi'
    delta_table = DeltaTable.forName(spark, silver_table)
    history_operation = (
        delta_table.history(limit=1)
        .select('operation', 'operationMetrics').collect()[0]
    )

    operation = history_operation[0]
    metrics = history_operation[1]

    if 'merge' in operation.lower():
        inserted = int(metrics['numTargetRowsInserted'])
        updated = int(metrics['numTargetRowsUpdated'])
        expected_source = int(metrics['numSourceRows'])

        print(f'''Inserted: {inserted} rows
Updated: {updated} rows
Expected source: {expected_source} rows
Source counted (from job): {source_count} rows''')
        
        assert expected_source == source_count
        assert updated + inserted <= source_count

    elif 'create' in operation.lower():
        print(f'''Inserted: {metrics["numOutputRows"]} rows
Source counted (from job): {source_count} rows''')

        assert int(metrics["numOutputRows"])==source_count

    print('\n=== Test pass ===\n')