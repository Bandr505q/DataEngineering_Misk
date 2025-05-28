from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

def extract(**kwargs):
    df = pd.read_csv('/home/bandr505/code/Projects/DataEngineering_Misk/laptopData.csv')
    kwargs['ti'].xcom_push(key='raw_df', value=df.to_json())

def transform(**kwargs):
    ti = kwargs['ti']
    raw_json = ti.xcom_pull(key='raw_df', task_ids='extract_task')
    df = pd.read_json(raw_json)

    df = df.drop_duplicates().dropna()

    def get_storage_type(x):
        if 'SSD' in x:
            return 'SSD'
        elif 'HDD' in x:
            return 'HDD'
        else:
            return 'Flash'

    df['Storage_Type'] = df['Memory'].apply(get_storage_type)

    if 'Unnamed: 0' in df.columns:
        df = df.drop(columns=['Unnamed: 0'])

    df['Price'] = (df['Price'].astype(int) / 10).astype(int)

    ti.xcom_push(key='clean_df', value=df.to_json())

def load(**kwargs):
    ti = kwargs['ti']
    clean_json = ti.xcom_pull(key='clean_df', task_ids='transform_task')
    df = pd.read_json(clean_json)
    df.to_csv('/home/bandr505/code/Projects/DataEngineering_Misk/clean_laptop.csv', index=False)

default_args = {
    'start_date': datetime(2025, 5, 28),
}

with DAG('etl_laptop_dag',
         schedule_interval=None,
         catchup=False,
         default_args=default_args,
         tags=['capstone'],
         description='ETL for laptop dataset') as dag:

    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract,
        provide_context=True
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load,
        provide_context=True
    )

    extract_task >> transform_task >> load_task
