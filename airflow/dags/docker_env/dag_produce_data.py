from airflow.sdk import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from airflow.models import Variable

from datetime import datetime

with DAG(
    dag_id='produce_data',
    dag_display_name='Produce Data',
    description='Run Produce data via Airflow',
    start_date=datetime(2026,1,1),
    schedule='*/2 * * * *',
    max_active_runs=1,  # chỉ cho 1 run chạy tại 1 thời điểm
    catchup=False,  # không backfill các run bị bỏ lỡ
) as dag:
    DockerOperator(
        task_id='run_container',
        api_version='auto',
        mount_tmp_dir=False,

        auto_remove='force',
        network_mode="ml-ops_default",
        docker_url="tcp://docker-proxy:2375",   # ref: https://medium.com/@benjcabalonajr_56579/using-docker-operator-on-airflow-running-inside-a-docker-container-7df5286daaa5
        image='longnb32/spark_env:v1.0',
        container_name='ctn_produce_data',
        working_dir='/app',
        mounts=[
            Mount(
                target='/app/src/utils',
                source='/home/longnb32/FPT/ops/ml-ops/src/utils',
                type='bind',
                read_only=True
            ),
            Mount(
                target='/app/spark',
                source='/home/longnb32/FPT/ops/ml-ops/spark',
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

            "PYTHONPATH": '/app/src',
            "PYTHONUNBUFFERED": 1
        },
        command='python -m utils.produce_data',
    )
