import requests
import csv
from init_minio import get_minio_client
from datetime import datetime
import os
import pytz
import duckdb
from logger_config import setup_logger
from dotenv import load_dotenv

#logger configuration
# This logger will be used to log messages in the script
logger = setup_logger(__name__)



class CoinDataExtractor:
    def __init__(self, asset_id, start_year=2024,interval="d1"):
        self.asset_id = asset_id
        self.start_year = start_year
        self.end_date = int(datetime.now().year)
        self.interval = interval
        self.url = f"https://rest.coincap.io/v3/assets/{asset_id}/history"        


    # This method fetches historical coin data from the CoinCap API for the specified years and saves it as CSV files.
    def get_coin_data(self):
        utc_zone = pytz.UTC

        load_dotenv()
        with open(os.getenv("API_KEY_PATH"), "r") as f:
            api_key = f.read().strip()
        #Api token
        headers = {
            "Authorization": f"Bearer {api_key}"
        }
        self.file_names = []
        for year in range(self.start_year, self.end_date + 1):
            file_name = f"{self.asset_id}-{year}"
            self.file_names.append(file_name)
            start_date = datetime(year,1,1)
            
            # Convert start_date to UTC timezone as API expects UTC timestamps
            start_of_year = utc_zone.localize(start_date.replace(hour=00, minute=00,second=00, microsecond=000000))
            end_of_year = utc_zone.localize(start_date.replace(month=12, day=31, hour=23, minute=59,second=59, microsecond=999999))
            
            start_timestamp = int(start_of_year.timestamp() * 1000)
            end_timestamp = int(end_of_year.timestamp() * 1000)

        
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
            try:
                data = response.json()
                assets = data["data"]
                keys = assets[0].keys()
                #Save the data to a CSV file
                with open(os.path.join("temp",file_name), "w") as file:
                    dict_writer = csv.DictWriter(file,fieldnames=keys)
                    dict_writer.writeheader()
                    dict_writer.writerows(assets)
                logger.info(f"Data successfully fetched for {self.asset_id}, year: {year}")
            except Exception as e:
                logger.error(f"Error fetching for {self.asset_id}, year: {year}") 
                raise



    # This method loads the data from the CSV file into Minio.
    def upload_file_to_minio(self):
        minio_client = get_minio_client()
        logger.info(f"file_names = {self.file_names}")
        for file_list in self.file_names:
            for file_name in file_list:
                local_path = os.path.join("temp",file_name)
                try:
                    minio_client.fput_object(self.asset_id, file_name, local_path)
                    os.remove(local_path)
                    logger.info(f"File uploaded to Minio successfully")
                except Exception as e:
                    logger.error(f"Error uploading file to Minio: {e}")
                    raise
        
    # This method creates a bucket in MinIO if it does not already exist.
    def create_bucket(self): 
        minio_client = get_minio_client()
        try:
            if not minio_client.bucket_exists(self.asset_id):
                minio_client.make_bucket(self.asset_id)
            logger.info(f"Succesfully bucket {self.asset_id} was created")
        except Exception as e:
            logger.error(f"Error creating bucket {self.asset_id}")
            raise
    
    

            
    # #This method returns the last load date from DuckDB if it exists
    # # If no data exists, it provides information to perform the initial load.
    # def get_last_load(self):
    #     db_path = os.path.join("data","CoinCap.db")
    #     conn = duckdb.connect(db_path)
    #     date_df = conn.execute("SELECT MAX(date_id) FROM history_price").fetchdf()
    #     try:
    #         if not date_df.empty:
    #             date = date_df.iloc[0,0]
    #             next_day = date + timedelta(days=1)
    #             next_day_str = next_day.strftime("%Y-%m-%d")
    #             logger.info(f"Last load date: {date.strftime('%Y-%m-%d')}")
    #             logger.info(f"Next load date: {next_day_str}")
    #             return next_day_str
    #         else:
    #             logger.info("No data found in DuckDB. Please first run the initial load.")
    #     finally:
    #         conn.close()
 
    
    # This method downloads CSV files from MinIO and loads the data into DuckDB's staging and history tables.
    def load_data_from_minio_to_duckdb(self, db_name: str = "CoinCap.db"):
        minio_client = get_minio_client()
        for file_list in self.file_names:
            logger.info(file_list)
            for file_name in file_list:
                try:
                    
                    local_file_path = os.path.join("temp",file_name)
                    
                    #init duckdb connection
                    db_path = os.path.join("data",db_name)
                    conn = duckdb.connect(db_path)

                    # #load file from minio
                    minio_client.fget_object(self.asset_id,file_name,local_file_path)

                    
                    list_of_variables =[self.asset_id,local_file_path]
                    print(list_of_variables)

                    #load into staging history_price
                    conn.execute("""
                        WITH temp_csv AS (
                            SELECT
                                $1 as asset_id,
                                priceUsd,
                                time,
                                date,
                                current_timestamp as load_timestamp
                            FROM read_csv($2)
                        )
                        INSERT INTO staging_history_price
                        SELECT *
                        FROM temp_csv
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM staging_history_price as s
                            WHERE s.asset_id = temp_csv.asset_id AND s.date = temp_csv.date
                        );
                    """, list_of_variables)

                    # remove records that have the same date as the currently loaded data
                    conn.execute("""
                        DELETE FROM history_price WHERE asset_id = $1 AND date_id IN (SELECT date FROM staging_history_price WHERE asset_Id = $1)""",[self.asset_id])

                    # inserting into history_price
                    conn.execute("""
                    INSERT INTO history_price
                    SELECT 
                        stag.asset_id,
                        dd.date_id,
                        priceUsd,
                        current_timestamp as load_timestamp
                    FROM staging_history_price as stag
                    JOIN dim_date as dd
                    ON dd.date_id = stag.date
                    JOIN dim_assets as da
                    ON da.asset_id = stag.asset_id
                    WHERE stag.asset_id = $1
                    """,[self.asset_id])
                    conn.commit()
                    conn.close()

                    #remove temporary file
                    os.remove(local_file_path)
                    logger.info(f"Data loaded successfully from {file_name} to history_price table")
                except Exception as e:
                    logger.error(f"Error loading data to history_price table: {e} from {file_name}")
                    raise