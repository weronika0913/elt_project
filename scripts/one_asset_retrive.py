import requests
import pandas as pd
from datetime import datetime
import os
import duckdb
from dotenv import load_dotenv
from logger_config import setup_logger
import json

# This logger will be used to log messages in the script
logger = setup_logger(__name__)


def load_assets():

    url = f"https://rest.coincap.io/v3/assets/bitcoin"
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
    with open("one_asset","w") as file:
        json.dump(data, file, indent=4)

load_assets()
        