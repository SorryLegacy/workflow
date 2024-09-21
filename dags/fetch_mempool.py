import asyncio

import pandas as pd
from sqlalchemy import create_engine, text
from httpx import AsyncClient
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime


async def fetch_mempool_data():
    async with AsyncClient() as client: 
        response = await client.get('https://mempool.space/api/v1/historical-price?cuurrency=USD')
        response.raise_for_status()
        return response.json()
    

def save_data_to_db(data):
    df = pd.DataFrame(data)
    print(df)
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/airflow', connect_args={"options": "-csearch_path=_raw_data"})
    with engine.connect() as con:
        con.execute(text('CREATE SCHEMA IF NOT EXISTS "_raw_data";'))
        df.columns = [column.lower() for column in df.columns]
        df = df[["time", "usd"]]
        df.to_sql('btc_price', con, if_exists='append', index=False)


def fetch_and_save_data():
    data = asyncio.run(fetch_mempool_data())
    save_data_to_db(data.get('prices'))


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 9, 21),
    'retries': 1,
}

with DAG('async_fetch_data_dag', 
         start_date=datetime(2023, 1, 1), 
         schedule='@daily', 
         catchup=False,
         default_args=default_args,
) as dag:
    fetch_data_task = PythonOperator(
        task_id='fetch_and_save_data',
        python_callable=fetch_and_save_data,
    )
    bitcoin_price_prediction = TriggerDagRunOperator(
        task_id='bitcoin_price_prediction',
        trigger_dag_id='bitcoin_price_prediction',
        wait_for_completion=True,
        dag=dag,
    )

    fetch_data_task >> bitcoin_price_prediction 
fetch_and_save_data()