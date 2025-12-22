"""
Databricks REST API Client

Shared HTTP client for all Databricks API operations.
"""
import os
import configparser
from pathlib import Path
from typing import Dict, Any, Optional
import requests


class DatabricksClient:
    """Client for making requests to Databricks REST APIs"""

    def __init__(
        self,
        host: Optional[str] = None,
        token: Optional[str] = None,
        profile: Optional[str] = None
    ):
        """
        Initialize Databricks client.

        Authentication priority:
        1. Explicit host/token parameters
        2. DATABRICKS_HOST and DATABRICKS_TOKEN env vars
        3. Profile from ~/.databrickscfg (use profile parameter or DATABRICKS_CONFIG_PROFILE env var)

        Args:
            host: Databricks workspace URL
            token: Databricks personal access token
            profile: Profile name from ~/.databrickscfg (e.g., "ai-strat")
        """
        # Try explicit parameters first
        self.host = host
        self.token = token

        # Try environment variables
        if not self.host:
            self.host = os.getenv("DATABRICKS_HOST", "")
        if not self.token:
            self.token = os.getenv("DATABRICKS_TOKEN", "")

        # Try config profile if still missing
        if not self.host or not self.token:
            profile_name = profile or os.getenv("DATABRICKS_CONFIG_PROFILE")
            if profile_name:
                profile_host, profile_token = self._load_profile(profile_name)
                if not self.host:
                    self.host = profile_host
                if not self.token:
                    self.token = profile_token

        # Strip trailing slash from host
        self.host = self.host.rstrip("/") if self.host else ""

        if not self.host or not self.token:
            raise ValueError(
                "Databricks host and token must be provided via:\n"
                "  1. Constructor parameters (host, token)\n"
                "  2. Environment variables (DATABRICKS_HOST, DATABRICKS_TOKEN)\n"
                "  3. Config profile (profile parameter or DATABRICKS_CONFIG_PROFILE env var)"
            )

        self.headers = {"Authorization": f"Bearer {self.token}"}

    @staticmethod
    def _load_profile(profile_name: str) -> tuple[str, str]:
        """
        Load credentials from ~/.databrickscfg profile.

        Args:
            profile_name: Profile name (e.g., "ai-strat")

        Returns:
            Tuple of (host, token)

        Raises:
            ValueError: If profile not found or missing required fields
        """
        config_path = Path.home() / ".databrickscfg"
        if not config_path.exists():
            raise ValueError(f"Databricks config file not found: {config_path}")

        config = configparser.ConfigParser()
        config.read(config_path)

        if profile_name not in config:
            available = ", ".join(config.sections())
            raise ValueError(
                f"Profile '{profile_name}' not found in {config_path}\n"
                f"Available profiles: {available}"
            )

        profile = config[profile_name]
        host = profile.get("host", "").strip()
        token = profile.get("token", "").strip()

        if not host or not token:
            raise ValueError(
                f"Profile '{profile_name}' is missing 'host' or 'token' field"
            )

        return host, token

    def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Make GET request to Databricks API.

        Args:
            endpoint: API endpoint path (e.g., "/api/2.1/unity-catalog/catalogs")
            params: Query parameters

        Returns:
            JSON response as dictionary

        Raises:
            requests.HTTPError: If request fails
        """
        url = f"{self.host}{endpoint}"
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()

    def post(self, endpoint: str, json: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Make POST request to Databricks API.

        Args:
            endpoint: API endpoint path
            json: JSON request body

        Returns:
            JSON response as dictionary

        Raises:
            requests.HTTPError: If request fails
        """
        url = f"{self.host}{endpoint}"
        response = requests.post(url, headers=self.headers, json=json)
        response.raise_for_status()
        return response.json()

    def patch(self, endpoint: str, json: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Make PATCH request to Databricks API.

        Args:
            endpoint: API endpoint path
            json: JSON request body

        Returns:
            JSON response as dictionary

        Raises:
            requests.HTTPError: If request fails
        """
        url = f"{self.host}{endpoint}"
        response = requests.patch(url, headers=self.headers, json=json)
        response.raise_for_status()
        return response.json()

    def delete(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """
        Make DELETE request to Databricks API.

        Args:
            endpoint: API endpoint path
            params: Query parameters

        Returns:
            JSON response as dictionary (if any)

        Raises:
            requests.HTTPError: If request fails
        """
        url = f"{self.host}{endpoint}"
        response = requests.delete(url, headers=self.headers, params=params)
        response.raise_for_status()

        # DELETE requests may not return content
        if response.content:
            return response.json()
        return None
