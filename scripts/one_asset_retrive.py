import requests
import os
from dotenv import load_dotenv
from logger_config import setup_logger
import json

# This logger will be used to log messages in the script
logger = setup_logger(__name__)


def load_assets():

    url = f"https://rest.coincap.io/v3/assets/bitcoin"
    load_dotenv()
    api_key = os.getenv("API_KEY")
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
        