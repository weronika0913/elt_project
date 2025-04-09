from airflow import DAG, Dataset
from airflow.operators.bash import BashOperator
from datetime import datetime
from pendulum import duration

default_args = {
    'owner': "airflow",
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}
DS_DUCKDB_BITCOIN = Dataset("duckdb://bitcoin")

with DAG('run_streamlit',
         default_args=default_args,
         schedule=[DS_DUCKDB_BITCOIN],
         catchup=False,
         dagrun_timeout=duration(hours=1),
         description="Run Streamlit app"
         ) as dag:

    run_streamlit_app = BashOperator(
        task_id='run_streamlit_app',
        bash_command='streamlit run /opt/airflow/visualization/dashboard.py --server.enableWebsocketCompression=false --server.enableCORS=false --server.port=8501',
    )
run_streamlit_app