import requests
import pandas as pd
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.exc import SQLAlchemyError
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook


def fetch_data(api_url):
    response = requests.get(api_url)
    return response.json()

def parse_data_to_dataframe(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='fetch_data')
    df = pd.DataFrame(data)
    return df

def insert_data_into_dwh(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids='parse_data_to_df')

    conn_uri = BaseHook.get_connection('postgres_conn').get_uri()
    if conn_uri.startswith("postgres://"):
        conn_uri = conn_uri.replace("postgres://", "postgresql://", 1)
    
    engine = create_engine(conn_uri)

    metadata = MetaData(bind=engine)
    table = Table('cannabis', metadata, schema='raw', autoload_with=engine)

    try:
        with engine.connect() as conn:
            existing_ids_query = conn.execute(f"SELECT id FROM raw.{table.name}")
            existing_ids = {row['id'] for row in existing_ids_query}
    except SQLAlchemyError as e:
        raise AirflowException(f"Error fetching existing IDs: {e}")
    
    new_data_df = df[~df['id'].isin(existing_ids)]

    with engine.connect() as conn:
        new_data_df.to_sql(table.name, conn, schema='raw', if_exists='append', index=False)