import requests
import csv
from init_minio import get_minio_client
from datetime import datetime
import os
import pytz
import duckdb
from logger_config import setup_logger
from auth import BaseApiAuth

class CoinCapExtractor:
    """
    The CoinCapExtractor class is designed to dynamically construct API endpoint URLs 
    for accessing cryptocurrency data from the CoinCap API. It allows users to specify 
    an endpoint template containing path parameters and optional query parameters.

    Attributes:
        endpoint (str):
          The specific API endpoint to call (e.g., 'history', 'rates').

        path_params (list): 
            A list of strings representing path values (e.g., asset IDs such as 'bitcoin', 'ethereum') 
            that will replace the `{path}` placeholder in the endpoint template.
        
        params (dict): 
            A dictionary of optional query parameters (e.g., {'interval': 'd1', 'start': ..., 'end': ...}).
    """
    def __init__(self, endpoint: str, path_params: list = None, params: dict = None):
        self.endpoint = endpoint
        self.path_params = path_params or []
        self.params = params or {}


    def _build_full_endpoint(self):
        """
    Iterates over the `path_params` list and formats the `endpoint` string by substituting 
        the `{path}` placeholder with each individual path value. 
        Returns a list of fully formatted endpoint URLs.

    Example:
            extractor = CoinCapExtractor(
                endpoint="assets/{path}/history",
                path_params=["bitcoin", "ethereum"]
            )
            urls = extractor._build_full_endpoint()
            # Output: ['assets/bitcoin/history', 'assets/ethereum/history']           
                """
        urls = []
        for path in self.path_params:
            url = self.endpoint.format(path=path)
            urls.append(url)
        return urls
    
    def _full_url(self):
        """
        Combines base URL from BaseApiAuth with formatted endpoints

        Returns:
            list[str]: A list of full URLs 
        """
        base = BaseApiAuth().full_url
        full_urls = []

        for endpoint in self._build_full_endpoint():
            url = f"{base}{endpoint}"
            full_urls.append(url)
        return full_urls
    
    def date_conversion(self):
        """
        Converts 'start' and 'end' date strings from the parameters dictionary to UNIX timestamps in milliseconds.

        This method expects the 'params' attribute to contain both 'start' and 'end' keys with date values 
        formatted as 'YYYY-MM-DD'. It validates the presence of these keys and parses the string dates into 
        timezone-aware UTC datetime objects. The resulting datetime objects are then converted to UNIX timestamps 
        in milliseconds (as required by the CoinCap API) and the original 'params' dictionary is updated accordingly.

        Raises:
            ValueError: If either 'start' or 'end' date is missing in the 'params' dictionary.

        Returns:
            dict: The updated 'params' dictionary with 'start' and 'end' values as UNIX time in milliseconds.
            """
        
        start_str = self.params.get("start")
        end_str = self.params.get("end")
        if not start_str or not end_str:
            raise ValueError("Missing 'start' or 'end' date in params attribute")
            
        else:
            start_date_obj = pytz.UTC.localize(datetime.strptime(start_str,"%Y-%m-%d"))
            end_date_obj = pytz.UTC.localize(datetime.strptime(end_str,"%Y-%m-%d"))

            self.params["start"] = int(start_date_obj.timestamp() * 1000)
            self.params["end"] = int(end_date_obj.timestamp() * 1000)
        return self.params
    
    def get_data(self):
        base = BaseApiAuth()
        token = base.get_token()
        if self.params.get("start") and self.params.get("end"):
                self.date_conversion()

        responses = []
        urls = self._full_url()

        for url in urls:
            response = requests.get(url,params=self.params,headers=token)
            data = response.json()
            responses.append(data)
        return responses



extractor = CoinCapExtractor(
    endpoint="v3/assets/{path}",
    path_params=["bitcoin", "ethereum"],
    params={"start": "2024-01-01", "end": "2024-01-02", "interval": "d1"}
)

data = extractor.get_data()

print(data)
