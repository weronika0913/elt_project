#Old second step of workflow

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os
from airflow import Dataset

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../scripts')))
#import functions from scripts
from extractor import CoinDataExtractor
from test_last_load import test_get_last_load_returns_string



asset_id = "bitcoin"
default_args = {
    'owner': "airflow",
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

def get_last_load():
    coin_extractor = CoinDataExtractor(asset_id)
    return coin_extractor.get_last_load()

def get_coin_data_task():
        last_load = get_last_load()
        coin_extractor = CoinDataExtractor(asset_id, last_load)
        coin_extractor.get_coin_data()

def upload_file_to_minio_task():
        last_load = get_last_load()
        coin_extractor = CoinDataExtractor(asset_id,last_load)
        coin_extractor.upload_file_to_minio()

def load_data_from_minio_to_duckdb_task():
        last_load = get_last_load()
        coin_extractor = CoinDataExtractor(asset_id,last_load)
        coin_extractor.load_data_from_minio_to_duckdb()

with DAG('elt_pipeline',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    

    last_load_test_string = PythonOperator(
          task_id = 'last_load_test_string',
          python_callable = test_get_last_load_returns_string
    )
    
    get_coin_API = PythonOperator(
        task_id = 'get_coin_data',
        python_callable = get_coin_data_task
    )

    upload_file_to_minio = PythonOperator(
        task_id='upload_file_to_minio',
        python_callable=upload_file_to_minio_task
    )

    load_data_from_minio_to_duckdb = PythonOperator(
        task_id='load_data_from_minio_to_duckdb',
        python_callable=load_data_from_minio_to_duckdb_task,
    )

last_load_test_string >> get_coin_API >> upload_file_to_minio >> load_data_from_minio_to_duckdb