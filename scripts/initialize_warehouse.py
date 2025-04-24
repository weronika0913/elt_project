import duckdb
import os
import pandas as pd
from logger_config import setup_logger

logger = setup_logger(__name__)

def create_tables():
     db_path = os.path.join("data","CoinCap.db")
     conn = duckdb.connect(db_path)
     try:
          conn.sql('''
                   
          CREATE OR REPLACE TABLE staging_history_price(
                    asset_id text,
                    priceUsd decimal(18,2),
                    time bigint,
                    date timestamp,
                    load_timestamp timestamp DEFAULT CURRENT_TIMESTAMP
                   );

          CREATE OR REPLACE TABLE dim_assets (
                    asset_id text,
                    name text,
                    rank int,
                    symbol text,
                    load_timestamp timestamp DEFAULT CURRENT_TIMESTAMP,
                    start_date timestamp,
                    end_date timestamp DEFAULT '9999-12-31',
                    is_current boolean DEFAULT TRUE,
                    PRIMARY KEY(asset_id, start_date)
                    );
          CREATE OR REPLACE TABLE dim_date (
               date_id timestamp PRIMARY KEY,
               year int,
               month int,
               day int,
               hour int
               
               );
          CREATE OR REPLACE TABLE history_price (
               asset_id text,
               date_id timestamp,
               price_usd decimal(18,2),
               load_timestamp timestamp DEFAULT CURRENT_TIMESTAMP,
               PRIMARY KEY(asset_id, date_id)
               );
                   
          CREATE OR REPLACE TABLE fact_coin_metrics (
               metric_id UUID,
               asset_id text,
               date_id date,
               supply decimal(18,2),
               maxSupply decimal(18,2),
               marketCapUsd decimal (18,2),
               priceUsd decimal (18,2),
               load_timestamp timestamp
               )
          ''') 
          logger.info("Tables dim_assets, dim_date, history_price has been successfully created")
     except Exception as e:
          logger.error(f"Error during creating dim_assets, dim_date, history_price: {e}")
          raise
     finally:
          conn.close()

def insert_into_date_table():
     db_path = os.path.join("data","CoinCap.db")
     conn = duckdb.connect(db_path)

     date_range = pd.date_range('2024-01-01', '2030-12-31', freq='h')

     generate_dim_date = pd.DataFrame({
          'date_id': date_range,
          'year': date_range.year,
          'month': date_range.month,
          'day': date_range.day,
          'hour': date_range.hour
     })
     try:
          conn.sql('''
               INSERT INTO dim_date BY NAME
               SELECT * from generate_dim_date      
                    ''')
          logger.info("Data has been succesfully loaded into dim_date")
     except Exception as e:
          logger.error(f"Error inserting into dim_date: {e}")
          raise
     finally:
          conn.close()
