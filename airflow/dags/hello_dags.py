from airflow.sdk import DAG, task
from airflow.providers.standard.operators.bash import BashOperator

from datetime import datetime
import os

with DAG(
    dag_id='hello_dags',
    dag_display_name='Run DAGs stuff',
    description='Run Bash/Task/Docker/... via Airflow',
    start_date=datetime(2026,1,1),
    schedule=None,
) as dag:
    task_bash_1 = BashOperator(
        task_id='say_hi',
        bash_command='echo "Hello ${my_name} from task <say_hi>"',
        env={
            'my_name': 'Bao Long'
        }
    )

    task_bash_2 = BashOperator(
        task_id='sleep_2',
        bash_command='echo "Sleep for 2s" && sleep 2 && echo "Just wake up from task sleep_5!"',
    )

    task_bash_3 = BashOperator(
        task_id='say_bye',
        bash_command='echo "Waiting for 3s" && sleep 3 && echo "Too late! Bye!"'
    )

    @task
    def get_env():
        print(f'AWS_ACCESS_KEY_ID: {os.getenv('AWS_ACCESS_KEY_ID')}')
        print(f'my_name: {os.getenv('my_name')}')
        print()

    task_bash_1 >> [task_bash_2, task_bash_3] >> get_env()
