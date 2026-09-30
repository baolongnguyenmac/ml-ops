# tôi nghĩ là không cần opt trên Bronze on schedule
# vì chẳng có job nào upsert vào đây
# và nó chỉ sinh nhiều file (small files problem) khi tôi xoá quá nhiều (lỗi khi ingest)

from delta.tables import DeltaTable

from bronze.ingest_bronze import SCHEMA, TABLE_BATCH, create_session


def optimize(target_table):
    delta_table = DeltaTable.forName(create_session(), target_table)

    delta_table.optimize().executeCompaction()

    # since I run my job every 2 minutes
    delta_table.vacuum(0.5)


if __name__=='__main__':
    # chạy 30p 1 lần để thấy được sự cải thiện (sau khi đã add 1 mớ file vào)
    optimize(target_table=f"{SCHEMA}.{TABLE_BATCH}")
