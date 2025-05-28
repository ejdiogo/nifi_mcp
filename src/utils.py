import os
# import logging # Remove standard logging
from loguru import logger # Import Loguru logger
import httpx
from dotenv import load_dotenv
import uuid # Import uuid for client ID generation
from typing import Optional, Dict, Any, Union, List, Literal # Add Union and List
from mcp.server.fastmcp.exceptions import ToolError # Import ToolError
import os

# Load environment variables from .env file
#load_dotenv()

# Set up logging - REMOVED standard logging setup
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

class NiFiAuthenticationError(Exception):
    """Raised when there is an error authenticating with NiFi."""
    pass

class NiFiClient:
    """A simple asynchronous client for the NiFi REST API."""

    def __init__(self, base_url: str, username: Optional[str] = None, password: Optional[str] = None, tls_verify: bool = True):
        """Initializes the NiFiClient.

        Args:
            base_url: The base URL of the NiFi API (e.g., "https://localhost:8443/nifi-api"). Required.
            username: The username for NiFi authentication. Required if password is provided.
            password: The password for NiFi authentication. Required if username is provided.
            tls_verify: Whether to verify the server's TLS certificate. Defaults to True.
        """
        if not base_url:
            raise ValueError("base_url is required for NiFiClient")
        self.base_url = base_url
        self.username = username
        self.password = password
        self.tls_verify = tls_verify
        self._client = None
        self._token = None
        # Generate a unique client ID for this instance, used for revisions
        self._client_id = str(uuid.uuid4())
        logger.info(f"NiFiClient initialized for {self.base_url} with client ID: {self._client_id}")

    @property
    def is_authenticated(self) -> bool:
        """Checks if the client currently holds an authentication token."""
        return self._token is not None

    async def _get_client(self):
        """Returns an httpx client instance, configuring auth if token exists."""
        # Always create a new client instance to ensure headers are fresh,
        # especially after authentication. If performance becomes an issue,
        # we could optimize, but this ensures correctness.
        if self._client:
             await self._client.aclose() # Ensure old connection is closed if recreating
             self._client = None

        headers = {}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
            # NiFi often requires client ID for state changes, let's check if we need it here
            # Might need to parse initial response or call another endpoint if needed.

        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            verify=self.tls_verify,
            headers=headers,
            timeout=30.0 # Keep timeout
        )
        return self._client
    
    async def authenticate(self):
        """Authenticates with NiFi and stores the token."""
        # Use a temporary client for the auth request itself, as it doesn't need the token header
        async with httpx.AsyncClient(base_url=self.base_url, verify=self.tls_verify, timeout=30.0) as auth_client:
            endpoint = "/access/token"
            try:
                logger.info(f"Authenticating with NiFi at {self.base_url}{endpoint}")
                logger.debug(f"Username: {self.username}")
                logger.debug(f"Password length: {len(self.password) if self.password else 0}")
                
                # Prepare the form data - according to NiFi 1.28.0 API docs
                # The endpoint expects: Content-Type: application/x-www-form-urlencoded
                # Parameters: username (formData), password (formData)
                form_data = {
                    "username": self.username,
                    "password": self.password
                }
                
                # Use httpx's built-in form encoding
                response = await auth_client.post(
                    endpoint,
                    data=form_data,  # httpx will automatically set Content-Type to application/x-www-form-urlencoded
                    timeout=30.0
                )
                
                logger.debug(f"Auth request URL: {auth_client.base_url}{endpoint}")
                logger.debug(f"Auth request Content-Type: application/x-www-form-urlencoded")
                logger.debug(f"Auth response status: {response.status_code}")
                logger.debug(f"Auth response headers: {dict(response.headers)}")
                logger.debug(f"Auth response content length: {len(response.content)}")
                
                if response.status_code != 200:
                    logger.error(f"Authentication failed with status {response.status_code}")
                    logger.error(f"Response text: {response.text}")
                    logger.error(f"Response headers: {dict(response.headers)}")
                
                response.raise_for_status()
                self._token = response.text.strip()  # Store the token and strip any whitespace
                logger.info("Authentication successful.")
                logger.debug(f"Token received (first 50 chars): {self._token[:50]}...")

                # Force recreation of the main client with the token on next call to _get_client
                if self._client:
                    await self._client.aclose()
                self._client = None

            except httpx.HTTPStatusError as e:
                error_details = {
                    "status_code": e.response.status_code,
                    "response_text": e.response.text,
                    "response_headers": dict(e.response.headers),
                    "request_url": str(e.request.url),
                    "request_method": e.request.method,
                }
                logger.error(f"Authentication failed: {error_details}")
                logger.debug(f"Request data sent: username={self.username}, password={'*' * len(self.password) if self.password else 'None'}")
                raise NiFiAuthenticationError(f"Authentication failed: {e.response.status_code} - {e.response.text}") from e
            except httpx.RequestError as e:
                logger.error(f"An error occurred during authentication: {e}")
                raise NiFiAuthenticationError(f"An error occurred during authentication: {e}") from e
            except Exception as e:
                logger.error(f"An unexpected error occurred during authentication: {e}", exc_info=True)
                raise NiFiAuthenticationError(f"An unexpected error occurred during authentication: {e}")
            
    def __repr__(self):
        return f"<{type(self).__name__} base_url={self.base_url} authenticated={self.is_authenticated}>"

async def get_nifi_client(base_url: str, username: str, password: str, tls_verify: bool) -> NiFiClient:
    """Returns a NiFiClient instance.
    
    Args:
        base_url: The base URL of the NiFi API
        username: The username for NiFi authentication
        password: The password for NiFi authentication
        tls_verify: If True, verifies TLS certificate. If False, disables verification.
                   Can also be a path to a CA bundle.
    """
    
    logger.info(f"Creating NiFi client with base_url: {base_url}")
    logger.debug(f"Username: {username}")
    logger.debug(f"Password provided: {bool(password)}")
    logger.debug(f"TLS verify: {tls_verify}")
    
    nifi = NiFiClient(
        base_url=base_url,
        username=username,
        password=password,
        tls_verify=tls_verify
    )
    await nifi.authenticate()
    
    return nifi
