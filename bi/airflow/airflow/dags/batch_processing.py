from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'football_data_processing_dag',
    default_args=default_args,
    description='DAG for Football Data Processing',
    schedule_interval=timedelta(days=1),  # Adjust the schedule interval as needed
)

# Replace these paths with the actual paths to your Python scripts
data_ingestion_script = '/home/fil_rouge/data_ingestion.py'
data_retrieval_script = '/home/fil_rouge/data_retrieval.py'
data_warehouse_script = '/home/fil_rouge/data_warehouse.py'

# Task to perform data ingestion
ingestion_task = BashOperator(
    task_id='data_ingestion',
    bash_command=f'python {data_ingestion_script}',
    dag=dag,
)

# Task to perform data retrieval
retrieval_task = BashOperator(
    task_id='data_retrieval',
    bash_command=f'python {data_retrieval_script}',
    dag=dag,
)

# Task to perform data warehouse processing
warehouse_task = BashOperator(
    task_id='data_warehouse',
    bash_command=f'python {data_warehouse_script}',
    dag=dag,
)

# Define the execution sequence
ingestion_task >> retrieval_task >> warehouse_task

if __name__ == "__main__":
    dag.cli()