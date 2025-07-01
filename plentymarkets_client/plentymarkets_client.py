import logging
import pickle
import time
from datetime import datetime
import requests


class PlentymarketsClient:
    """
    A simplified Python wrapper for the PlentyMarkets API.

    This class manages authentication, token handling, and provides basic GET/PUT request methods
    with automatic session handling. Tokens are stored locally to reduce login overhead.
    """

    def __init__(self, base_url: str, credentials: dict) -> None:
        """
        Initialize the PlentymarketsClient.

        Args:
            base_url (str): Base URL for the PlentyMarkets API.
            credentials (dict): Dictionary with 'username' and 'password'.
        """
        self.__log = logging.getLogger(__name__)
        self.__sanity_check_credentials(credentials)

        self.__py_api_user = credentials["username"]
        self.__py_api_password = credentials["password"]
        self.__py_api_base_url = base_url
        self.__py_api_token = None
        self.__py_api_refresh_token = None
        self.__py_api_token_expires_at = None

        self.__bootstrap()

    def __login(self):
        """Log in to PlentyMarkets API and save the access token."""
        response = requests.post(
            f"{self.__py_api_base_url}/rest/login?username={self.__py_api_user}&password={self.__py_api_password}"
        )
        json = response.json()
        if response.status_code != 200:
            error = json.get("error", str(response.status_code))
            raise RuntimeError(f"Login failed! {error}")
        self.__save_token(json)

    def __refresh_login(self):
        """Attempt to refresh the access token.

        Returns:
            bool: True if successful, False otherwise.
        """
        response = requests.post(
            f"{self.__py_api_base_url}/rest/login/refresh",
            headers=self.__generate_header(),
            data={"refresh_token": self.__py_api_refresh_token},
        )
        json = response.json()
        if response.status_code != 200:
            error = json.get("error", str(response.status_code))
            self.__log.error(f"Session refresh failed! {error} - Try to login again!")
            return False
        self.__save_token(json)
        return True

    def __save_token(self, token):
        """Store the access and refresh tokens locally.

        Args:
            token (dict): Token data from login response.

        Returns:
            bool: True if saved successfully, False otherwise.
        """
        expires_at = datetime.utcnow().timestamp() + token["expiresIn"] - (2 * 60 * 60)
        self.__py_api_token_expires_at = expires_at
        self.__py_api_token = token["access_token"]
        self.__py_api_refresh_token = token["refresh_token"]

        try:
            with open(".py_session", "wb") as fp:
                pickle.dump({
                    "access_token": token["access_token"],
                    "refresh_token": token["refresh_token"],
                    "expires_at": expires_at,
                }, fp)
        except Exception as e:
            self.__log.exception(e)
            return False
        return True

    def __load_token(self):
        """Load the stored token from local file.

        Returns:
            bool: True if loaded and valid, False otherwise.
        """
        try:
            with open(".py_session", "rb") as fp:
                token = pickle.load(fp)

            if not all(k in token for k in ("expires_at", "access_token", "refresh_token")):
                raise ValueError("Malformed Token")

            self.__py_api_token_expires_at = token["expires_at"]
            self.__py_api_token = token["access_token"]
            self.__py_api_refresh_token = token["refresh_token"]

        except Exception as e:
            self.__log.exception(e)
            return False
        return True

    def __bootstrap(self):
        """Initialize or refresh session."""
        if not self.__load_token():
            self.__log.info("No existing PlentyMarkets session found!")
            self.__login()
            return

        if datetime.utcnow().timestamp() > self.__py_api_token_expires_at:
            self.__log.info("Session expired, trying to refresh")
            if not self.__refresh_login():
                self.__login()

    def __sanity_check_credentials(self, credentials: dict) -> None:
        """Validate credentials format.

        Args:
            credentials (dict): Must include 'username' and 'password'.

        Raises:
            ValueError: If validation fails.
        """
        if not isinstance(credentials, dict):
            raise ValueError("credentials has to be dict")
        if "username" not in credentials:
            raise ValueError("Missing username")
        if "password" not in credentials:
            raise ValueError("Missing API-Password")

    def __generate_header(self, options=None):
        """Generate authorization header.

        Returns:
            dict: HTTP header with Bearer token.
        """
        return {
            "accept": "application/json",
            "Authorization": f"Bearer {self.__py_api_token}"
        }

    def __simple_get_request(self, route, params=None, url_params=None, return_binary=False):
        """Perform a GET request with optional retry logic.

        Args:
            route (str): API route.
            params (dict, optional): URL parameters.
            url_params (dict, optional): Path parameters.
            return_binary (bool): If True, returns raw content.

        Returns:
            dict|bytes|None: JSON response or binary content.
        """
        delay = 5
        backoff = 20
        endpoint = self.__build_endpoint(route, url_params)

        for _ in range(3):
            try:
                response = requests.get(endpoint, params=params, headers=self.__generate_header())

                if response.status_code == 200:
                    return response.content if return_binary else response.json()
                if response.status_code == 401:
                    self.__log.warning(f"Unauthenticated - retrying in {delay}s")
                    time.sleep(delay)
                    if not self.__refresh_login():
                        self.__login()
                elif response.status_code == 429:
                    self.__log.warning(f"Rate limit exceeded - retrying in {delay}s")
                    time.sleep(delay)
            except Exception as e:
                self.__log.error(f"Exception during GET request: {e}")
            delay += backoff
        return None

    def __put_request(self, route, url_params=None, params=None, json=None):
        """Perform a PUT request with retry logic.

        Args:
            route (str): API route.
            url_params (dict, optional): Path parameters.
            params (dict, optional): Query parameters.
            json (dict, optional): Request payload.

        Returns:
            bool: True on success.
        """
        delay = 5
        backoff = 20
        endpoint = self.__build_endpoint(route, url_params)

        for _ in range(3):
            response = requests.put(endpoint, params=params, json=json, headers=self.__generate_header())
            if response.status_code == 200:
                return True
            if response.status_code == 401:
                self.__log.warning(f"Unauthenticated - retrying in {delay}s")
                time.sleep(delay)
                if not self.__refresh_login():
                    self.__login()
            if response.status_code == 429:
                self.__log.warning(f"Rate limited - retrying in {delay}s")
                time.sleep(delay)
            delay += backoff
        return False

    def __paginated_get_request(self, route, params={}, json=None, url_params=None):
        """Fetch all pages of a paginated endpoint.

        Args:
            route (str): API route.
            params (dict): URL parameters.
            json (dict, optional): JSON payload.
            url_params (dict, optional): Path parameters.

        Returns:
            list: All entries from paginated response.
        """
        entries = []
        stop = False
        page_number = 1

        if "itemsPerPage" not in params:
            params.update({"itemsPerPage": 100})

        while not stop:
            response = self.__simple_get_request(route, params, url_params=url_params)
            if isinstance(response, (dict, list)) and "error" in response:
                return response

            self.__log.info(f"Page {page_number} / {response['lastPageNumber']} fetched")
            entries += response["entries"]
            stop = response["isLastPage"]
            page_number = response["page"] + 1
            params["page"] = page_number
        return entries

    def __curser_get_request(self, route, params={}, json=None, url_params=None):
        """Fetch all entries using cursor-based pagination.

        Args:
            route (str): API route.
            params (dict): URL parameters.
            json (dict, optional): JSON payload.
            url_params (dict, optional): Path parameters.

        Returns:
            list: All entries.
        """
        entries = []
        stop = False
        cursor = None

        while not stop:
            response = self.__simple_get_request(route, params, url_params=url_params)
            if isinstance(response, (dict, list)) and "error" in response:
                return response
            entries += response["entries"]
            cursor = response.get("cursor")
            stop = not response["entries"]
            params["cursor"] = cursor
            self.__log.debug(f"cursor: {cursor} - Fetched {len(entries)} entries")
        return entries

    def __build_endpoint(self, route, url_params=None):
        """Construct full endpoint URL.

        Args:
            route (str): Named route key.
            url_params (dict, optional): Replacement parameters.

        Returns:
            str: Full URL.
        """
        routes = {
            "download_documents_by_type": "/rest/orders/documents/downloads/{type}"
        }
        route = f"{self.__py_api_base_url}{routes[route]}"
        return route.format(**url_params) if url_params else route

    def get_documents_by_type(self, doc_type, createdAtFrom=None, createdAtTo=None, batchSize=None, page=1):
        """Download documents by type with optional filters.

        Args:
            doc_type (str): Document type identifier.
            createdAtFrom (str, optional): Start date filter (ISO format).
            createdAtTo (str, optional): End date filter (ISO format).
            batchSize (int, optional): Number of items per page.
            page (int, optional): Page number.

        Returns:
            bytes|None: Raw ZIP data or None.
        """
        params = {
            "page": page
        }
        if createdAtFrom:
            params["createdAtFrom"] = createdAtFrom
        if createdAtTo:
            params["createdAtTo"] = createdAtTo
        if batchSize:
            params["itemsPerPage"] = batchSize

        return self.__simple_get_request(
            "download_documents_by_type",
            url_params={"type": doc_type},
            params=params,
            return_binary=True
        )
