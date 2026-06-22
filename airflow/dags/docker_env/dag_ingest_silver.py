from airflow.sdk import DAG, Asset
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from airflow.models import Variable

from datetime import datetime

bronze_asset = Asset("s3://flezi-data/bronze/")
silver_asset = Asset("s3://flezi-data/silver/")

with DAG(
    dag_id='ingest_silver',
    dag_display_name='Ingest Silver',
    description='Run Ingest Silver via Airflow',
    start_date=datetime(2026,1,1),
    schedule=[bronze_asset],
    max_active_runs=1,  # chỉ cho 1 run chạy tại 1 thời điểm
    catchup=False,  # không backfill các run bị bỏ lỡ
) as dag:
    DockerOperator(
        task_id='silver.ingest_silver',
        api_version='auto',
        mount_tmp_dir=False,
        outlets=[silver_asset],

        auto_remove='force',
        network_mode="ml-ops_default",
        docker_url="tcp://docker-proxy:2375",   # ref: https://medium.com/@benjcabalonajr_56579/using-docker-operator-on-airflow-running-inside-a-docker-container-7df5286daaa5
        image='longnb32/spark_env:v1.1',
        container_name='ctn_ingest_silver',
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

            "LH_METASTORE_HOST": Variable.get("LH_METASTORE_HOST"),

            "PYTHONPATH": '/app/src',
            "PYTHONUNBUFFERED": 1
        },
        command='python -m silver.ingest_silver',
    )
