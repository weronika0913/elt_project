from minio import Minio
from dotenv import load_dotenv
import os
from logger_config import setup_logger

logger = setup_logger(__name__)

# Load environment variables from .env file
load_dotenv()
access_key_id = os.getenv("ACCESS_KEY_ID")
secret_access_key = os.getenv("SECRET_ACCESS_KEY")
endpoint_url = os.getenv("ENDPOINT_URL")

# This function initializes the Minio client and checks the connection to the Minio server.
def get_minio_client():
    try:
        minio_client = Minio(
                        endpoint_url.replace("http://","").replace("https://",""),
                        access_key=access_key_id,
                        secret_key=secret_access_key,
                        secure=False
                            )
        minio_client.list_buckets()
        logger.info("Minio client connected successfully")
        return minio_client
    except Exception as e:
        logger.error(f"Error connecting to Minio client: {e}")
        raise
