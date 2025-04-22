#First step on project workflow


from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os
from airflow import Dataset

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../scripts')))
#import functions from scripts
from scripts import initialize_warehouse
from scripts import fetch_all_assets


""""
 This DAG is designed to run once and initialize the ELT pipeline for the warehouse.
"""

default_args = {
    'owner': "airflow",
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}


with DAG('init_warehouse',
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:
    
    create_tables = PythonOperator(
        task_id = 'create_tables',
        python_callable =  initialize_warehouse.create_tables
    )

    fill_out_date_table = PythonOperator(
        task_id = 'initialize_date_table',
        python_callable = initialize_warehouse.insert_into_date_table
    )
    

    fill_out_asset_table = PythonOperator(
        task_id = 'initialize_asset_table',
        python_callable = fetch_all_assets.load_assets
    )

create_tables >> fill_out_date_table >> fill_out_asset_table

