from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.utils.dates import days_ago
from scripts.fifteen_bucks_elt import fetch_data, parse_data_to_dataframe, insert_data_into_dwh

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'start_date': days_ago(1)
}

API_URL = 'https://random-data-api.com/api/cannabis/random_cannabis?size=10'

with DAG(
    dag_id='DAG_FIFTEEN_BUCKS_ELT',
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

    t1 = HttpSensor(
        task_id='wait_for_cannabis_api',
        http_conn_id=None,
        endpoint=API_URL,
        method='GET',
        response_check=lambda response: response.status_code == 200,
        poke_interval=90,
        timeout=20,
        dag=dag
    )

    t2 = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data,
        op_kwargs={'api_url': API_URL},
        dag=dag
    )

    t3 = PythonOperator(
        task_id='parse_data_to_df',
        python_callable=parse_data_to_dataframe,
        provide_context=True,
        dag=dag
    )

    t4 = PythonOperator(
        task_id='insert_data_into_dwh',
        python_callable=insert_data_into_dwh,
        provide_context=True,
        dag=dag
    )

    dummy_end = DummyOperator(
        task_id='end_dummy',
        dag=dag
    )

    dummy_start >> t1 >> t2 >> t3 >> t4 >> dummy_end

