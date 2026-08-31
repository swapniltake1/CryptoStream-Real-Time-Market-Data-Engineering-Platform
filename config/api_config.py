# ============================================================================
# CryptoStream - API Configuration
# ============================================================================

from dataclasses import dataclass
from typing import Dict, List, Optional
import os


@dataclass
class APIConfig:
    """CoinGecko API Configuration"""
    
    # Base API Configuration
    BASE_URL: str = "https://api.coingecko.com/api/v3"
    MARKET_ENDPOINT: str = "/coins/markets"
    COIN_DETAIL_ENDPOINT: str = "/coins/{id}"
    
    # Request Configuration
    TIMEOUT_SECONDS: int = 30
    PER_PAGE: int = 250
    MAX_RETRIES: int = 3
    
    # Retry Policy
    INITIAL_RETRY_DELAY: float = 2.0  # seconds
    MAX_RETRY_DELAY: float = 60.0     # seconds
    EXPONENTIAL_BASE: float = 2.0     # exponential backoff multiplier
    
    # Rate Limiting (CoinGecko Free Tier)
    RATE_LIMIT_CALLS: int = 50        # calls per minute
    RATE_LIMIT_WINDOW: int = 60       # seconds
    
    # Circuit Breaker Configuration
    CIRCUIT_BREAKER_THRESHOLD: int = 5  # failures before opening circuit
    CIRCUIT_BREAKER_TIMEOUT: int = 300  # seconds before retry
    
    # API Authentication (if using Pro API)
    API_KEY_SECRET_SCOPE: str = "crypto-api-keys"
    API_KEY_SECRET_KEY: str = "coingecko-api-key"
    USE_API_KEY: bool = False  # Set to True for Pro API
    
    # Request Headers
    USER_AGENT: str = "CryptoStream/1.0 (Databricks Pipeline)"
    
    # Monitoring
    LOG_API_CALLS: bool = True
    TRACK_RESPONSE_TIME: bool = True
    

@dataclass
class EndpointConfig:
    """Configuration for specific API endpoints"""
    
    MARKETS_PARAMS: Dict[str, any] = None
    
    def __post_init__(self):
        if self.MARKETS_PARAMS is None:
            self.MARKETS_PARAMS = {
                "order": "market_cap_desc",
                "per_page": 250,
                "page": 1,
                "sparkline": False,
                "price_change_percentage": "1h,24h,7d,30d"
            }


# ============================================================================
# HTTP Status Code Handling
# ============================================================================

RETRYABLE_STATUS_CODES = [
    408,  # Request Timeout
    429,  # Too Many Requests
    500,  # Internal Server Error
    502,  # Bad Gateway
    503,  # Service Unavailable
    504   # Gateway Timeout
]

NON_RETRYABLE_STATUS_CODES = [
    400,  # Bad Request
    401,  # Unauthorized
    403,  # Forbidden
    404   # Not Found
]


# ============================================================================
# Error Messages
# ============================================================================

ERROR_MESSAGES = {
    "TIMEOUT": "Request timed out after {timeout} seconds",
    "RATE_LIMIT": "Rate limit exceeded. Retry after {retry_after} seconds",
    "CONNECTION": "Failed to establish connection to API",
    "AUTH": "Authentication failed. Check API credentials",
    "NOT_FOUND": "Requested resource not found",
    "SERVER_ERROR": "API server error. Status code: {status_code}",
    "UNKNOWN": "Unexpected error occurred: {error}"
}


# ============================================================================
# Validation Rules
# ============================================================================

class ValidationRules:
    """API response validation rules"""
    
    REQUIRED_FIELDS = [
        "id",
        "symbol",
        "name",
        "current_price",
        "market_cap",
        "last_updated"
    ]
    
    NUMERIC_FIELDS = [
        "current_price",
        "market_cap",
        "total_volume",
        "price_change_24h",
        "price_change_percentage_24h"
    ]
    
    MIN_RESPONSE_SIZE = 1  # Minimum number of records expected
    MAX_RESPONSE_SIZE = 250  # Maximum per page


# ============================================================================
# Instance Creation
# ============================================================================

def get_api_config() -> APIConfig:
    """Get API configuration instance"""
    return APIConfig()

def get_endpoint_config() -> EndpointConfig:
    """Get endpoint configuration instance"""
    return EndpointConfig()