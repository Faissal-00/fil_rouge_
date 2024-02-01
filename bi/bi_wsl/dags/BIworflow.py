from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from datetime import datetime
import requests

dag = DAG(
    'bi_dag',
    default_args={'start_date': days_ago(1)},
    schedule_interval='0 23 * * *',
    catchup=False
)

# Tasks

def run_app_script():
    # Assuming you've mounted the WSL filesystem to /wsl_mount in your Docker container
    exec(open("/home/fil_rouge/app.py").read())

# PythonOperator for running the 'app.py' script
app_task = PythonOperator(
    task_id='run_app_script',
    python_callable=run_app_script,
    dag=dag
)

if __name__ == "__main__":
    dag.cli()