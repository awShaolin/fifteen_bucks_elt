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
    
    engine = create_engine(BaseHook.get_connection('postgres_conn').get_uri())

    metadata = MetaData(bind=engine)
    table = Table('raw_cannabis', metadata, autoload_with=engine)

    try:
        with engine.connect() as conn:
            existing_ids_query = conn.execute(f"SELECT id FROM {table.name}")
            existing_ids = {row['id'] for row in existing_ids_query}
    except SQLAlchemyError as e:
        raise AirflowException(f"Error fetching existing IDs: {e}")
    
    new_data_df = df[~df['id'].isin(existing_ids)]

    with engine.connect() as conn:
        new_data_df.to_sql(table.name, conn, if_exists='append', index=False)