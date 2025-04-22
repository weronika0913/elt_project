#Second step on project workflow


from airflow import DAG
from airflow.operators.python import PythonOperator
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
    def process_coin(coin: str):
        extractor = CoinDataExtractor(coin)
        extractor.get_coin_data()
    
    @task
    def create_bucket(coin: str):
        extractor = CoinDataExtractor(coin)
        extractor.create_bucket()

    @task 
    def upload_file_to_minio(coin: str):
        extractor = CoinDataExtractor(coin)
        extractor.upload_file_to_minio()
    
    @task 
    def load_data_from_minio_to_duckdb(coin: str):
        extractor = CoinDataExtractor(coin)
        extractor.load_data_from_minio_to_duckdb()


    
    coin_list = ['binance-coin','bitcoin','ethereum']

    # Dynamic mapping 
    create = create_bucket.expand(coin=coin_list)
    process = process_coin.expand(coin=coin_list)
    upload = upload_file_to_minio.expand(coin=coin_list)
    load = load_data_from_minio_to_duckdb.expand(coin=coin_list)

    create >> process >> upload >> load