import os
from datetime import datetime, timezone

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG, Asset, Variable
from docker.types import Mount

bronze_asset = Asset(os.path.join(Variable.get("S3_URI"), 'bronze'))
silver_asset = Asset(os.path.join(Variable.get("S3_URI"), 'silver'))
gold_asset = Asset("jdbc:postgresql://company_db:5432/gold")

with DAG(
    dag_id='ingest_gold',
    dag_display_name='Gold Modeling',
    description='Modeling data from Silver to Gold zone',
    start_date=datetime(2026,1,1, tzinfo=timezone.utc),
    schedule=[silver_asset],
    # schedule='*/2 * * * *',
    max_active_runs=1,  # chỉ cho 1 run chạy tại 1 thời điểm
    catchup=False,  # không backfill các run bị bỏ lỡ
    fail_fast=True, # stop dag if a task fails
) as dag:
    gold_setup = DockerOperator(
        task_id='gold.setup',
        api_version='auto',
        mount_tmp_dir=False,

        auto_remove='force',
        network_mode="ml-ops_default",
        docker_url="tcp://docker-proxy:2375",   # ref: https://medium.com/@benjcabalonajr_56579/using-docker-operator-on-airflow-running-inside-a-docker-container-7df5286daaa5
        image='longnb32/spark_env:v1.4',
        container_name='ctn_gold_setup',
        working_dir='/app',
        mounts=[
            Mount(
                target='/app/src',
                source='/home/nblong/Dropbox/Project/ops/ml-ops/src',
                type='bind',
                read_only=True
            ),
            Mount(
                target='/app/sql',
                source='/home/nblong/Dropbox/Project/ops/ml-ops/sql',
                type='bind',
                read_only=True
            )
        ],
        environment={
            "COM_DB_HOST": Variable.get("COM_DB_HOST"),
            "COM_GOLD_DB_NAME": Variable.get("COM_GOLD_DB_NAME"),
            "COM_DB_PORT": Variable.get("COM_DB_PORT"),
            "COM_DB_USER": Variable.get("COM_DB_USER"),
            "COM_DB_PASSWORD": Variable.get("COM_DB_PASSWORD"),

            "PYTHONPATH": '/app/src',
            "PYTHONUNBUFFERED": 1
        },
        command='python -m gold.setup_gold',
    )

    seed_setup = DockerOperator(
        task_id='gold.seed',
        api_version='auto',
        mount_tmp_dir=False,

        auto_remove='force',
        network_mode="ml-ops_default",
        docker_url="tcp://docker-proxy:2375",   # ref: https://medium.com/@benjcabalonajr_56579/using-docker-operator-on-airflow-running-inside-a-docker-container-7df5286daaa5
        image='longnb32/spark_env:v1.4',
        container_name='ctn_gold_seed',
        working_dir='/app/src/gold/dbt_gold',
        mounts=[
            Mount(
                target='/app/src/gold/dbt_gold',
                source='/home/nblong/Dropbox/Project/ops/ml-ops/src/gold/dbt_gold',
                type='bind',
                read_only=False
            )
        ],
        environment={
            "PYTHONUNBUFFERED": 1
        },
        command=['bash', '-c', 'dbt seed'],
    )

    staging_ingestion = DockerOperator(
        task_id='gold.staging.ingest',
        api_version='auto',
        mount_tmp_dir=False,
        outlets=[gold_asset],

        auto_remove='force',
        network_mode="ml-ops_default",
        docker_url="tcp://docker-proxy:2375",   # ref: https://medium.com/@benjcabalonajr_56579/using-docker-operator-on-airflow-running-inside-a-docker-container-7df5286daaa5
        image='longnb32/spark_env:v1.4',
        container_name='ctn_gold_staging_ingest',
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
            "COM_GOLD_DB_NAME": Variable.get("COM_GOLD_DB_NAME"),

            "LH_METASTORE_HOST": Variable.get("LH_METASTORE_HOST"),

            "PYTHONPATH": '/app/src',
            "PYTHONUNBUFFERED": 1
        },
        command=['bash', '-c', 'python -m gold.ingest_staging'],
    )

    gold_model = DockerOperator(
        task_id='gold.model',
        api_version='auto',
        mount_tmp_dir=False,

        auto_remove='force',
        network_mode="ml-ops_default",
        docker_url="tcp://docker-proxy:2375",   # ref: https://medium.com/@benjcabalonajr_56579/using-docker-operator-on-airflow-running-inside-a-docker-container-7df5286daaa5
        image='longnb32/spark_env:v1.4',
        container_name='ctn_gold_model',
        working_dir='/app/src/gold/dbt_gold',
        mounts=[
            Mount(
                target='/app/src/gold/dbt_gold',
                source='/home/nblong/Dropbox/Project/ops/ml-ops/src/gold/dbt_gold',
                type='bind',
                read_only=False
            )
        ],
        environment={
            "PYTHONUNBUFFERED": 1
        },
        command=['bash', '-c', 'pwd && dbt snapshot && dbt run && dbt test'],
    )

    gold_setup >> [staging_ingestion, seed_setup] >> gold_model
    # dbt_transform
