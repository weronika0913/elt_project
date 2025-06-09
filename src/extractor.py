"""
Module responsible for extracting cryptocurrency data from the CoinCap API.

This module provides the `CoinCapExtractor` class that helps build dynamic API endpoints
with path and query parameters, convert date parameters to UNIX timestamps, and fetch data
from CoinCap API with proper authentication.

Classes:
    CoinCapExtractor:
        Builds endpoint URLs dynamically based on asset IDs and parameters,
        converts date parameters, and handles API requests with authorization.
"""

from datetime import datetime
import requests
import pytz
from .auth import BaseApiAuth


# pylint: disable=too-few-public-methods
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
        if self.path_params:
            for path in self.path_params:
                url = self.endpoint.format(path=path)
                urls.append(url)
            return urls
        return [self.endpoint]

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

    def _date_conversion(self):
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

        start_date_obj = pytz.UTC.localize(datetime.strptime(start_str, "%Y-%m-%d"))
        end_date_obj = pytz.UTC.localize(datetime.strptime(end_str, "%Y-%m-%d"))

        self.params["start"] = int(start_date_obj.timestamp() * 1000)
        self.params["end"] = int(end_date_obj.timestamp() * 1000)
        return self.params

    def get_data(self):
        """
        Fetches data from the CoinCap API for all constructed endpoint URLs.

        This method builds full URLs based on the given endpoint and path parameters,
        converts date parameters if present, sends GET requests with proper headers,
        and collects JSON responses from the API.

        Raises:
            ValueError: If the API response status is not successful or the JSON decoding fails.

        Returns:
            list: A list of JSON-decoded responses from the API for each URL.
        """
        base = BaseApiAuth()
        token = base.get_token()
        if self.params.get("start") or self.params.get("end"):
            self._date_conversion()

        responses = []
        urls = self._full_url()
        for url in urls:
            response = requests.get(url, params=self.params, headers=token, timeout=30)
            try:
                response.raise_for_status()
                data = response.json()
                responses.append(data)
            except requests.exceptions.JSONDecodeError as e:
                raise ValueError(f"Failed to decode JSON -> {url}") from e
            except requests.exceptions.RequestException as e:
                raise ValueError(f"Error fetching data: {e} -> {url}") from e
        return responses
