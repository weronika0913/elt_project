#Old first step of workflow

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os
from airflow import Dataset

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../scripts')))
#import functions from scripts
from extractor import CoinDataExtractor

""""
 This DAG is designed to run once and initialize the ELT pipeline for the Bitcoin dataset.
"""

DS_DUCKDB_BITCOIN = Dataset("duckdb://bitcoin")

asset_id = "bitcoin"
default_args = {
    'owner': "airflow",
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

def get_coin_data_task():
        coin_extractor = CoinDataExtractor(asset_id)
        coin_extractor.get_coin_data()

def create_bucket_task():
        coin_extractor = CoinDataExtractor(asset_id)
        coin_extractor.create_bucket()

def upload_file_to_minio_task():
        coin_extractor = CoinDataExtractor(asset_id)
        coin_extractor.upload_file_to_minio()

def load_data_from_minio_to_duckdb_task():
        coin_extractor = CoinDataExtractor(asset_id)
        coin_extractor.load_data_from_minio_to_duckdb()

with DAG('init_elt_pipeline',
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:


    get_coin_API = PythonOperator(
        task_id = 'get_coin_data',
        python_callable = get_coin_data_task
    )

    create_bucket = PythonOperator(
        task_id='create_bucket',
        python_callable=create_bucket_task
    
    )
    upload_file_to_minio = PythonOperator(
        task_id='upload_file_to_minio',
        python_callable=upload_file_to_minio_task
    )

    load_data_from_minio_to_duckdb = PythonOperator(
        task_id='load_data_from_minio_to_duckdb',
        python_callable=load_data_from_minio_to_duckdb_task,
        outlets=[DS_DUCKDB_BITCOIN] 
    )

# Define the task dependencies
get_coin_API >> create_bucket >> upload_file_to_minio >> load_data_from_minio_to_duckdb



