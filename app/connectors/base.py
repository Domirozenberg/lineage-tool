from abc import ABC, abstractmethod
from enum import Enum
from typing import Any


class AuthMode(str, Enum):
    OFFLINE = "offline"
    USERNAME_PASSWORD = "username_password"
    API_KEY = "api_key"
    OAUTH = "oauth"
    SAML = "saml"
    KEY_FILE = "key_file"
    SERVICE_ACCOUNT = "service_account"


class BaseConnector(ABC):
    """Abstract base class for all metadata extractors."""

    def __init__(self, config: dict[str, Any], auth_mode: AuthMode = AuthMode.OFFLINE):
        self.config = config
        self.auth_mode = auth_mode

    @abstractmethod
    def test_connection(self) -> bool:
        """Verify connectivity to the data source."""

    @abstractmethod
    def extract_metadata(self) -> dict[str, Any]:
        """Extract metadata from the data source."""

    @abstractmethod
    def extract_lineage(self) -> dict[str, Any]:
        """Extract lineage relationships from the data source."""
