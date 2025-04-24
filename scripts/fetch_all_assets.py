import requests
import pandas as pd
from datetime import datetime
import os
import duckdb
from dotenv import load_dotenv
from logger_config import setup_logger

# This logger will be used to log messages in the script
logger = setup_logger(__name__)


def load_assets():

    url = f"https://rest.coincap.io/v3/assets"
    load_dotenv()
    with open(os.getenv("API_KEY_PATH"), "r") as f:
        api_key = f.read().strip()
    #Api token
    headers = {
        "Authorization": f"Bearer {api_key}"
        }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data: {e}")
        return

    data = response.json()
    assets = data["data"]
    df = pd.DataFrame(assets)

    df_filtered = df[['id','name','rank','symbol']].copy()
    df_filtered.loc[:,'start_date'] = datetime.now()


    db_path = os.path.join('data','CoinCap.db')
    conn = duckdb.connect(db_path)
    try:
        conn.sql('''
            INSERT INTO dim_assets BY Name
            SELECT 
                id as asset_id,
                name,
                rank,
                symbol,
                current_timestamp as load_timestamp,
                start_date,
                '9999-12-31' as end_date,
                TRUE as is_current
                FROM df_filtered
                ''')
        logger.info("Data succesfully loaded into dim_assets")
    except Exception as e:
        logger.error(f"Error during load data into dim_assets: {e}")
        raise
    finally:
        conn.close()





#   #   #   #   #
#  TO revision SCD2
#   test data
#   #   #   #   #
new_data = pd.DataFrame([
    {"asset_id": "bitcoin", "name": "Bitcoin", "symbol": "BTC", "rank": 1},
    {"asset_id": "ethereum", "name": "Ethereum Updated", "symbol": "ETH", "rank": 2},  # name changed
])



def scd2():
    new_data = pd.DataFrame(new_data)
    new_data['start_date'] = datetime.now
    new_data['load_timestamp'] = datetime.now
    new_data['end_date'] = '9999-12-31'
    new_data['is_current'] = True

    db_path = os.path.join('data','CoinCap.db')
    conn = duckdb.connect(db_path)
    existing = conn.sql('SELECT * FROM dim_assets WHERE is_current = TRUE').fetchdf()
    for _, new_row in new_data.iterrows:
        asset_id = new_row['asset_id']

        # Find current record for this asset
        current = existing['asset_id'] == asset_id

        # If no existing record, just insert
        if current.empty:
            conn.sql('INSERT INTO dim_assets SELECT * FROM new_data WHERE asset_id=?', [asset_id])
        else:
            # Check if name or symbol changed
            curr_row = current.iloc[0]
            if curr_row['name'] != new_row['name'] or curr_row['symbol'] != new_row['symbol']:
                # Update old record
                conn.sql('''
                         UPDATE dim_assets
                         SET end_date = ? AND is_current = TRUE
                        ''',[datetime.now(),asset_id])
                # Insert new version
                conn.sql('INSERT INTO dim_assets SELECT * FROM new_data WHERE asset_id = ?', [asset_id])

