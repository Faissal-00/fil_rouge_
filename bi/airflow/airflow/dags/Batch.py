from airflow import DAG

from airflow.operators.python_operator import PythonOperator

from airflow.utils.dates import days_ago

from datetime import datetime

import requests



def print_welcome():

    print('Welcome to Airflow!')



def print_date():

    print('Today is {}'.format(datetime.today().date()))



def print_random_quote():

    response = requests.get('https://api.quotable.io/random')

    quote = response.json()['content']

    print('Quote of the day: "{}"'.format(quote))



dag = DAG(

    'welcome_dag',

    default_args={'start_date': days_ago(1)},

    schedule_interval='0 23 * * *',

    catchup=False

)



data_ingestion = PythonOperator(

    task_id='data_ingestion',

    python_callable=print_welcome,

    dag=dag

)



data_retrieval = PythonOperator(

    task_id='data_retrieval',

    python_callable=print_date,

    dag=dag

)



data_warehouse = PythonOperator(

    task_id='data_warehouse',

    python_callable=print_random_quote,

    dag=dag

)



# Set the dependencies between the tasks

data_ingestion >> data_retrieval >> data_warehouse