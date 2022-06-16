from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

args = {
    'owner': 'quessand',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
    }

with DAG(
    "db_update", 
    default_args=args, 
    schedule_interval=None) as dag:

    with TaskGroup("Preparing_source_data") as group1:
        t1 = BashOperator(
            task_id='get_source_data',
            bash_command='python3 "/lessons/migrations/Этап 0. Подготовка исходных данных/get_source_data.py"')

        t2 = BashOperator(
            task_id='upload_to_stage',
            bash_command='python3 "/lessons/migrations/Этап 0. Подготовка исходных данных/upload_to_stage.py"')

        t3 = BashOperator(
            task_id='migrate_to_mart',
            bash_command='python3 "/lessons/migrations/Этап 0. Подготовка исходных данных/migrate_to_mart.py"')

        t1 >> t2 >> t3

    with TaskGroup("Incrementation_of_new_data") as group2:
        g2_url = '/lessons/migrations/Этап 1. Транзакции'
        t4 = BashOperator(
            task_id='get_increment',
            bash_command=f'python3 "{g2_url}/get_increment.py"')

        t5 = BashOperator(
            task_id='upload_increment_to_stage',
            bash_command=f'python3 "{g2_url}/upload_increment_to_stage.py"')

        t6 = BashOperator(
            task_id='update_mart',
            bash_command=f'python3 "{g2_url}/update_mart.py"')
        
        t4 >> t5 >> t6
    
    t7 = BashOperator(
        task_id='update_schema',
        bash_command=f'python3 "{g2_url}/update_schema.py"')

    t8 = BashOperator(
        task_id='create_view',
        bash_command=f'python3 "/lessons/migrations/Этап 2. Customer retention/create_view.py"')

    group1 >> t7 >> group2 >> t8