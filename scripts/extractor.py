import requests
import csv
from init_minio import get_minio_client
from datetime import datetime, timedelta
import os
import pytz
import duckdb
from logger_config import setup_logger
from dotenv import load_dotenv

#logger configuration
# This logger will be used to log messages in the script
logger = setup_logger(__name__)



class CoinDataExtractor:
    def __init__(self, asset_id, start_date="2021-01-01",interval="h1"):
        self.asset_id = asset_id
        self.start_date = datetime.strptime(start_date,"%Y-%m-%d")
        self.interval = interval
        self.url = f"https://rest.coincap.io/v3/assets/{asset_id}/history"
        self.file_name = f"{asset_id}-{start_date}"
        


    # This method fetches the coin data from the API and saves it to a CSV file.
    def get_coin_data(self):
        
        # Convert start_date to UTC timezone as API expects UTC timestamps
        utc_zone = pytz.UTC
        start_of_day = utc_zone.localize(self.start_date.replace(hour=0, minute=0, second=0, microsecond=0))
        end_of_day = utc_zone.localize(self.start_date.replace(hour=23, minute=59, second=59, microsecond=999999))
        
        start_timestamp = int(start_of_day.timestamp() * 1000)
        end_timestamp = int(end_of_day.timestamp() * 1000)

        load_dotenv()
        with open(os.getenv("API_KEY_PATH"), "r") as f:
            api_key = f.read().strip()
        #Api token
        headers = {
            "Authorization": f"Bearer {api_key}"
        }

        #parameters for the API request
        params = {
            "interval": self.interval,
            "start": start_timestamp,
            "end": end_timestamp
        }
        try:
            response = requests.get(self.url, params=params, headers=headers)
            response.raise_for_status()
        
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data: {e}")
            return
        data = response.json()
        assets = data["data"]
        keys = assets[0].keys()
        #Save the data to a CSV file
        with open(os.path.join("temp",self.file_name), "w") as file:
            dict_writer = csv.DictWriter(file,fieldnames=keys)
            dict_writer.writeheader()
            dict_writer.writerows(assets)    

    # This method loads the data from the CSV file into Minio.
    def upload_file_to_minio(self):
        minio_client = get_minio_client()
        local_path = os.path.join("temp",self.file_name)
        try:
            minio_client.fput_object(self.asset_id, self.file_name, local_path)
            os.remove(local_path)
            logger.info(f"File uploaded to Minio successfully")
        except Exception as e:
            logger.error(f"Error uploading file to Minio: {e}")
    
    #This method create a bucket in minio if it doesn't exist
    def create_bucket(self): 
        minio_client = get_minio_client()
        if not minio_client.bucket_exists(self.asset_id):
            minio_client.make_bucket(self.asset_id)

            
    #This method returns the last load date from DuckDB if it exists
    # If it doesn't exist, it returns the start date of the data extraction
    def get_last_load(self):
        db_path = os.path.join("data","historical_bitcoin.db")
        conn = duckdb.connect(db_path)
        date_df = conn.execute("SELECT MAX(date) FROM bitcoin").fetchdf()
        try:
            if not date_df.empty:
                date = date_df.iloc[0,0]
                next_day = date + timedelta(days=1)
                next_day_str = next_day.strftime("%Y-%m-%d")
                logger.info(f"Last load date: {date.strftime('%Y-%m-%d')}")
                logger.info(f"Next load date: {next_day_str}")
                return next_day_str
            else:
                logger.info("No data found in DuckDB. Please first run the initial load.")
        finally:
            conn.close()
    
    #This method loads the data from Minio to DuckDB
    # It creates a table if it doesn't exist and loads the data into it.
    def load_data_from_minio_to_duckdb(self, db_name: str = "historical_bitcoin.db"):
        try:
            minio_client = get_minio_client()
            local_file_path = os.path.join("temp",self.file_name)
            
            #init duckdb connection
            db_path = os.path.join("data",db_name)
            conn = duckdb.connect(db_path)

            #load file from minio
            minio_client.fget_object(self.asset_id, self.file_name,local_file_path)

            #create table if not exists
            conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.asset_id} AS
            SELECT 
                priceUSD,
                time,
                circulatingSupply,
                date
            FROM read_csv('{local_file_path}', header=true,auto_detect=True)
            """)

            #load data to table
            conn.execute(f"""
            INSERT INTO {self.asset_id}
            SELECT 
                priceUSD,
                time,
                circulatingSupply,
                date
            FROM read_csv('{local_file_path}', header=true,auto_detect=True)
            """)
            conn.commit()
            conn.close()

            #remove temporary file
            os.remove(local_file_path)
            logger.info(f"Data loaded to DuckDB successfully")
        except Exception as e:
            logger.error(f"Error loading data to DuckDB: {e}")
            raise
