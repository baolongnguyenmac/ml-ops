from delta.tables import DeltaTable

from silver.ingest_silver import SCHEMA, TABLE_BATCH, create_session


def optimize(target_table:str, key:str):
    delta_table = DeltaTable.forName(create_session(), target_table)

    delta_table.optimize().executeCompaction()

    # tập các column được z-order và tập các column được partition không được phép giao nhau
    # vì trong 1 partition, các gia trị của các column được dùng để partition là giống nhau
    # --> z-order trên các cột này là vô nghĩa
    (
        delta_table
        .optimize()
        .executeZOrderBy(key)
    )

    # since I run my job every 2 minutes
    delta_table.vacuum(0.5)


if __name__=='__main__':
    # chạy 30p 1 lần để thấy được sự cải thiện (sau khi đã add 1 mớ file vào)
    optimize(target_table=f'{SCHEMA}.{TABLE_BATCH}', key='record_key')
