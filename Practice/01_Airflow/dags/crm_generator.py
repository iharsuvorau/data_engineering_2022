from datetime import timedelta, datetime

import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator, task

DATA_FOLDER = '/tmp/data'


@task
def fetch_users(ti=None):
    api_url = 'https://randomuser.me/api/'

    users = []
    while len(users) < 5:
        response = requests.get(api_url)
        if response.status_code == 200:
            users.append(response.json()['results'][0])

    ti.xcom_push(key='users', value=users)


@task
def process_users(output_path=DATA_FOLDER, ti=None):
    users = ti.xcom_pull(key='users', task_ids='fetch_users')

    df = pd.json_normalize(users)

    df.to_csv(f'{output_path}/users.csv', index=False)


with DAG(
        dag_id='crm_generator',
        schedule_interval=timedelta(minutes=1),
        start_date=datetime(2022, 9, 25, 21, 40),
        catchup=False,
        template_searchpath='/tmp/data',
        default_args={
            'owner': 'Ihar',
            'depends_on_past': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        }
) as dag:
    fetch_users() >> process_users()
