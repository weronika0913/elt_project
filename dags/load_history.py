#Second step on project workflow


from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain
from datetime import datetime
import sys
import os
from airflow.decorators import task

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../scripts')))
#import functions from scripts
from extractor import CoinDataExtractor


""""
 This DAG is designed to run once and fill out history data for history_price dataset.
"""

default_args = {
    'owner': "airflow",
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}


with DAG('load_history',
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:
    
    @task
    def process_coin(coin: str,ti=None):
        extractor = CoinDataExtractor(coin)
        extractor.get_coin_data()
        ti.xcom_push(key=f'file_names_{coin}', value=extractor.file_names)
    
    @task
    def create_bucket(coin: str):
        extractor = CoinDataExtractor(coin)
        extractor.create_bucket()

    @task 
    def upload_file_to_minio(coin: str, ti=None):
        file_names = ti.xcom_pull(key=f'file_names_{coin}')
        extractor = CoinDataExtractor(coin)
        extractor.file_names = file_names
        extractor.upload_file_to_minio()
    
    @task 
    def load_data_from_minio_to_duckdb(coin: str,ti=None):
        file_names = ti.xcom_pull(key=f'file_names_{coin}')
        extractor = CoinDataExtractor(coin)
        extractor.file_names = file_names
        extractor.load_data_from_minio_to_duckdb()


    
    coin_list = ['binance-coin','bitcoin','ethereum']
    load_tasks = []
    # Dynamic mapping 
    for coin in coin_list:
        create = create_bucket.override(task_id=f"create_{coin}").expand(coin=[coin])
        process = process_coin.override(task_id=f"process_{coin}").expand(coin=[coin])
        upload = upload_file_to_minio.override(task_id=f"upload_{coin}").expand(coin=[coin])
        load = load_data_from_minio_to_duckdb.override(task_id=f"load_{coin}").expand(coin=[coin])

        # Sequence for one coin
        chain(create, process, upload, load)
        load_tasks.append(load)

    # Sequence for loads - manual linking one by one
        for i in range(len(load_tasks) - 1):
            load_tasks[i] >> load_tasks[i + 1]