from datetime import datetime, timezone

from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG
from docker.types import Mount

with DAG(
    dag_id='optimization',
    dag_display_name='Optimize Delta Lake',
    description='Optimize Delta Lake',
    start_date=datetime(2026,1,1, tzinfo=timezone.utc),
    schedule='*/30 * * * *',
    max_active_runs=1,  # chỉ cho 1 run chạy tại 1 thời điểm
    catchup=False,  # không backfill các run bị bỏ lỡ
) as dag:
    DockerOperator(
        task_id='silver.optimization',
        api_version='auto',
        mount_tmp_dir=False,

        auto_remove='force',
        network_mode="ml-ops_default",
        docker_url="tcp://docker-proxy:2375",   # ref: https://medium.com/@benjcabalonajr_56579/using-docker-operator-on-airflow-running-inside-a-docker-container-7df5286daaa5
        image='longnb32/spark_env:v1.3',
        container_name='ctn_optimization',
        working_dir='/app',
        mounts=[
            Mount(
                target='/app/src',
                source='/home/longnb32/FPT/ops/ml-ops/src',
                type='bind',
                read_only=True
            )
        ],
        environment={
            "AWS_ACCESS_KEY_ID": Variable.get("AWS_ACCESS_KEY_ID"),
            "AWS_SECRET_ACCESS_KEY": Variable.get("AWS_SECRET_ACCESS_KEY"),

            "LH_METASTORE_HOST": Variable.get("LH_METASTORE_HOST"),

            "PYTHONPATH": '/app/src',
            "PYTHONUNBUFFERED": 1
        },
        command='python -m bronze.optimize_bronze || python -m silver.optimize_silver'
    )