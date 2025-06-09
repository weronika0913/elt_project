"""
Module responsible for user authentication logic.
"""

import os
from dotenv import load_dotenv
from minio import Minio


# pylint: disable=too-few-public-methods
class BaseApiAuth:
    """
    BaseApiAuth provides a base interface for API-based authentication

    Arg:
        url: base URL of the API
        endpoint: the url endpoint from which data will be extracted

    """

    def __init__(self, url: str = "https://rest.coincap.io/"):
        self.full_url = url

    def get_token(self, token_name: str = "API_KEY") -> dict[str, str]:
        """
        Retrive token details from env file

        Arg:
            token_name: name of variable where Api Key is stored

        Raises:
            ValueError: if the api_key is none

        Return:
            headers as dict
        """
        load_dotenv()
        api_key = os.getenv(token_name)
        if api_key is None:
            raise ValueError(f"{token_name} environment variable is not set")
        return {"Authorization": f"Bearer {api_key}"}


class BaseMinioAuth:
    """
    Base Minio authentication for connection

    Loads credentials from environment variables:
        - ACCESS_KEY_ID
        - SECRET_ACCESS_KEY
        - ENDPOINT_URL

    """

    def __init__(self):
        auth = self.get_auth()

        self.access_key_id = auth["access_key_id"]
        self.secret_access_key = auth["secret_access_key"]
        self.endpoint_url = auth["endpoint_url"]

    def get_auth(self):
        """
        Return a dictionary containing Minio connection credentials

        Variables:
         - access_key_id:
         - secret_access_key:
         - endpoint_url:
        """
        load_dotenv()
        return {
            "access_key_id": os.getenv("ACCESS_KEY_ID"),
            "secret_access_key": os.getenv("SECRET_ACCESS_KEY"),
            "endpoint_url": os.getenv("ENDPOINT_URL"),
        }

    def get_client(self):
        """
        Initialize Minio client
        """
        minio_client = Minio(
            self.endpoint_url.replace("http://", "").replace("https://", ""),
            access_key=self.access_key_id,
            secret_key=self.secret_access_key,
            secure=False,
        )
        return minio_client
