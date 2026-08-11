import os
from datetime import datetime, timezone

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

with DAG(
    dag_id='hello_dags',
    dag_display_name='Run DAGs stuff',
    description='Run Bash/Task/Docker/... via Airflow',
    start_date=datetime(2026,1,1, tzinfo=timezone.utc),
    fail_fast=True,
    schedule='*/1 * * * *',
) as dag:
    task_bash_1 = BashOperator(
        task_id='task_bash_1',
        bash_command='echo "Hello ${my_name} from task <say_hi>"',
        env={
            'my_name': 'Bao Long' 
        }
    )

    task_bash_2 = BashOperator(
        task_id='task_bash_2',
        bash_command='exit 0',
    )

    task_bash_3 = BashOperator(
        task_id='task_bash_3',
        bash_command='echo "Waiting for 3s" && sleep 3 && echo "Too late! Bye!"'
    )

    @task
    def get_env():
        print(f'AWS_ACCESS_KEY_ID: {os.getenv('AWS_ACCESS_KEY_ID')}')
        print(f'my_name: {os.getenv('my_name')}')
        print(1/0)

    @task
    def print_hello_world():
        print('hello world')

    task_bash_1 >> [task_bash_3, task_bash_2] >> get_env() >> print_hello_world()
