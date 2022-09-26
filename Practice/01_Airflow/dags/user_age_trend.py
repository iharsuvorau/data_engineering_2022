from datetime import timedelta, datetime
from os import path

import pandas as pd
from airflow.decorators import task
from airflow.models.dag import dag
from airflow.operators.python import BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.filesystem import FileSensor
from sqlalchemy import create_engine

DATA_FOLDER = '/tmp/data'
CSV_PATH = f'{DATA_FOLDER}/users.csv'
DB_TABLE_NAME = 'crm_users'


@dag(
    dag_id='user_age_trend',
    schedule_interval=timedelta(minutes=1),
    start_date=datetime(2022, 9, 25, 19, 40),
    catchup=False,
    template_searchpath=DATA_FOLDER,
    default_args={
        'owner': 'Ihar',
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1)
    }
)
def user_age_trend():
    pg_hook = PostgresHook(postgres_conn_id='airflow_pg')
    engine = pg_hook.get_sqlalchemy_engine()

    wait_for_file = FileSensor(
        task_id='wait_for_file',
        filepath=CSV_PATH,
        fs_conn_id='fs_default'
    )

    @task
    def insert_users():
        df = pd.read_csv(CSV_PATH)
        df['processed_timestamp'] = pd.Timestamp.now()

        df.to_sql(
            DB_TABLE_NAME,
            con=engine,
            if_exists='append',
            index=False,
            method='multi',
        )

    @task
    def compute_mean_age():
        df = pd.read_sql_table(DB_TABLE_NAME, con=engine)
        return df['dob.age'].mean()

    @task
    def save_mean_age(avg_age):
        with open(f'{DATA_FOLDER}/avg_age.txt', 'w') as f:
            f.write(str(avg_age))

    @task
    def compare_mean_age(avg_age):
        age_path = f'{DATA_FOLDER}/avg_age.txt'

        if not path.exists(age_path):
            return -1

        with open(age_path, 'r') as f:
            prev_avg_age = float(f.read())

        print(f'Previous avg age: {prev_avg_age}, current avg age: {avg_age}, comparison: {avg_age > prev_avg_age}')

        if avg_age > prev_avg_age:
            return 0
        elif avg_age < prev_avg_age:
            return 1
        elif avg_age == prev_avg_age:
            return 2

    options = ['age_increased', 'age_decreased', 'has_not_changed', 'previous_unknown']

    def choose_option(trend_index):
        trend_index = int(trend_index)
        if trend_index == -1:
            return options[3]
        elif trend_index == 0:
            return options[0]
        elif trend_index == 1:
            return options[1]
        elif trend_index == 2:
            return options[2]

    choose_option_task = BranchPythonOperator(
        task_id='choose_option',
        python_callable=choose_option,
        op_kwargs={'trend_index': '{{ task_instance.xcom_pull(task_ids="compare_mean_age") }}'}
    )

    for option in options:
        @task(task_id=option)
        def print_option(task_id):
            print(task_id)

        print_option_task = print_option(option)
        choose_option_task >> print_option_task

    mean_age = compute_mean_age()
    trend = compare_mean_age(mean_age)

    wait_for_file >> insert_users() >> mean_age >> trend >> save_mean_age(mean_age) >> choose_option_task


dag = user_age_trend()
