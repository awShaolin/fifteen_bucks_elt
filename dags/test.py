from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import datetime as dt

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'start_date': days_ago(1)
}

with DAG(
    dag_id='cannabis_load',
    default_args=default_args,
    description='This dag make API request and load data to raw.cannabis table',
    tags=['ELT'],
    schedule_interval=None,
    catchup=False,
    max_active_runs=1
) as dag:

    dummy_start = DummyOperator(
        task_id='start_dummy',
        dag=dag
    )

    t1 = PostgresOperator(
        task_id='table_check',
        depends_on_past=False,
        postgres_conn_id='postgres_conn',
        sql="select 1 from raw.cannabis;",
        dag=dag
    )

    dummy_end = DummyOperator(
        task_id = 'end_dummy',
        dag=dag
    )

    dummy_start >> t1 >> dummy_end