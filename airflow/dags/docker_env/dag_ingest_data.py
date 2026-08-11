from datetime import datetime, timezone

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, Variable
from docker.types import Mount

with DAG(
    dag_id='ingestion',
    dag_display_name='Bronze >> Silver >> Gold',
    description='Walk data through Bronze >> Silver >> Gold',
    start_date=datetime(2026,1,1, tzinfo=timezone.utc),
    schedule='*/2 * * * *',
    max_active_runs=1,  # chỉ cho 1 run chạy tại 1 thời điểm
    catchup=False,  # không backfill các run bị bỏ lỡ
    fail_fast=True, # stop dag if a task fails
) as dag:
    bronze_ingestion = DockerOperator(
        task_id='bronze.ingest_bronze',
        api_version='auto',
        mount_tmp_dir=False,

        auto_remove='force',
        network_mode="ml-ops_default",
        docker_url="tcp://docker-proxy:2375",   # ref: https://medium.com/@benjcabalonajr_56579/using-docker-operator-on-airflow-running-inside-a-docker-container-7df5286daaa5
        image='longnb32/spark_env:v1.3',
        container_name='ctn_ingest_bronze',
        working_dir='/app',
        mounts=[
            Mount(
                target='/app/src',
                source='/home/nblong/Dropbox/Project/ops/ml-ops/src',
                type='bind',
                read_only=True
            ),
            Mount(
                target='/app/tracking',
                source='/home/nblong/Dropbox/Project/ops/ml-ops/tracking',
                type='bind'
            ),
        ],
        environment={
            "AWS_ACCESS_KEY_ID": Variable.get("AWS_ACCESS_KEY_ID"),
            "AWS_SECRET_ACCESS_KEY": Variable.get("AWS_SECRET_ACCESS_KEY"),
            "S3_URI": Variable.get("S3_URI"),

            "COM_DB_HOST": Variable.get("COM_DB_HOST"),
            "COM_DB_PORT": Variable.get("COM_DB_PORT"),
            "COM_DB_USER": Variable.get("COM_DB_USER"),
            "COM_DB_PASSWORD": Variable.get("COM_DB_PASSWORD"),
            "COM_DB_NAME": Variable.get("COM_DB_NAME"),

            "LH_METASTORE_HOST": Variable.get("LH_METASTORE_HOST"),

            "PYTHONPATH": '/app/src',
            "PYTHONUNBUFFERED": 1
        },
        command='python -m bronze.ingest_bronze',
    )

    silver_ingestion = DockerOperator(
        task_id='silver.ingest_silver',
        api_version='auto',
        mount_tmp_dir=False,

        auto_remove='force',
        network_mode="ml-ops_default",
        docker_url="tcp://docker-proxy:2375",   # ref: https://medium.com/@benjcabalonajr_56579/using-docker-operator-on-airflow-running-inside-a-docker-container-7df5286daaa5
        image='longnb32/spark_env:v1.3',
        container_name='ctn_ingest_silver',
        working_dir='/app',
        mounts=[
            Mount(
                target='/app/src',
                source='/home/nblong/Dropbox/Project/ops/ml-ops/src',
                type='bind',
                read_only=True
            ),
            Mount(
                target='/app/tracking',
                source='/home/nblong/Dropbox/Project/ops/ml-ops/tracking',
                type='bind'
            ),
        ],
        environment={
            "AWS_ACCESS_KEY_ID": Variable.get("AWS_ACCESS_KEY_ID"),
            "AWS_SECRET_ACCESS_KEY": Variable.get("AWS_SECRET_ACCESS_KEY"),
            "S3_URI": Variable.get("S3_URI"),

            "LH_METASTORE_HOST": Variable.get("LH_METASTORE_HOST"),

            "PYTHONPATH": '/app/src',
            "PYTHONUNBUFFERED": 1
        },
        command='python -m silver.ingest_silver',
    )

    # start = EmptyOperator(task_id="start_pipeline")
    # end = EmptyOperator(task_id="end_pipeline")
    # start >> bronze_ingestion >> silver_ingestion >> end
    bronze_ingestion >> silver_ingestion
