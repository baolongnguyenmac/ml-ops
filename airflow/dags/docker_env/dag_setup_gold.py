from datetime import datetime, timezone

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Variable
from docker.types import Mount

with DAG(
    dag_id='setup_gold',
    dag_display_name='Setup Gold zone',
    description='Setup Gold zone',
    start_date=datetime(2026,1,1, tzinfo=timezone.utc),
    schedule=None,
    max_active_runs=1,  # chỉ cho 1 run chạy tại 1 thời điểm
    catchup=False,  # không backfill các run bị bỏ lỡ
    fail_fast=True, # stop dag if a task fails
) as dag:
    setup_dim_date = DockerOperator(
        task_id='gold.setup_gold',
        api_version='auto',
        mount_tmp_dir=False,

        auto_remove='force',
        network_mode="ml-ops_default",
        docker_url="tcp://docker-proxy:2375",   # ref: https://medium.com/@benjcabalonajr_56579/using-docker-operator-on-airflow-running-inside-a-docker-container-7df5286daaa5
        image='longnb32/spark_env:v1.3',
        container_name='ctn_setup_gold',
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

    setup_deps_seed = DockerOperator(
        task_id='deps_seed',
        api_version='auto',
        mount_tmp_dir=False,

        auto_remove='force',
        network_mode="ml-ops_default",
        docker_url="tcp://docker-proxy:2375",   # ref: https://medium.com/@benjcabalonajr_56579/using-docker-operator-on-airflow-running-inside-a-docker-container-7df5286daaa5
        image='longnb32/spark_env:v1.3',
        container_name='ctn_seed_gold',
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
        command=['bash', '-c', 'pwd && dbt deps && dbt seed'],
    )

    [setup_dim_date, setup_deps_seed]
