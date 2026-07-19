from airflow.sdk import DAG, Asset
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from airflow.models import Variable

from datetime import datetime

bronze_asset = Asset("s3://flezi-data/bronze/")

with DAG(
    dag_id='ingest_bronze',
    dag_display_name='Ingest Bronze',
    description='Run Ingest Bronze via Airflow',
    start_date=datetime(2026,1,1),
    schedule='*/2 * * * *',
    max_active_runs=1,  # chỉ cho 1 run chạy tại 1 thời điểm
    catchup=False,  # không backfill các run bị bỏ lỡ
) as dag:
    DockerOperator(
        task_id='bronze.ingest_bronze',
        api_version='auto',
        mount_tmp_dir=False,
        outlets=[bronze_asset],

        auto_remove='force',
        network_mode="ml-ops_default",
        docker_url="tcp://docker-proxy:2375",   # ref: https://medium.com/@benjcabalonajr_56579/using-docker-operator-on-airflow-running-inside-a-docker-container-7df5286daaa5
        image='longnb32/spark_env:v1.3',
        container_name='ctn_ingest_bronze',
        working_dir='/app',
        mounts=[
            Mount(
                target='/app/src',
                source='/home/longnb32/FPT/ops/ml-ops/src',
                type='bind',
                read_only=True
            ),
            Mount(
                target='/app/tracking',
                source='/home/longnb32/FPT/ops/ml-ops/tracking',
                type='bind'
            ),
        ],
        environment={
            "AWS_ACCESS_KEY_ID": Variable.get("AWS_ACCESS_KEY_ID"),
            "AWS_SECRET_ACCESS_KEY": Variable.get("AWS_SECRET_ACCESS_KEY"),

            "COM_DB_HOST": Variable.get("COM_DB_HOST"),
            "COM_DB_PORT": Variable.get("COM_DB_PORT"),
            "COM_DB_USER": Variable.get("COM_DB_USER"),
            "COM_DB_PASSWORD": Variable.get("COM_DB_PASSWORD"),
            "COM_DB_NAME": Variable.get("COM_DB_NAME"),

            "DL_METASTORE_HOST": Variable.get("DL_METASTORE_HOST"),

            "PYTHONPATH": '/app/src',
            "PYTHONUNBUFFERED": 1
        },
        command='python -m bronze.ingest_bronze',
    )
