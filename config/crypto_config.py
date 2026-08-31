# ============================================================================
# CryptoStream - Cryptocurrency Configuration
# ============================================================================

from dataclasses import dataclass, field
from typing import List, Dict
from enum import Enum


class CryptoCurrency(Enum):
    """Target currencies for market data"""
    USD = "usd"
    EUR = "eur"
    GBP = "gbp"
    JPY = "jpy"
    BTC = "btc"
    ETH = "eth"


class CoinCategory(Enum):
    """Coin categories for organization"""
    MAJOR = "major"  # Top 10 by market cap
    LARGE_CAP = "large_cap"  # Top 11-50
    MID_CAP = "mid_cap"  # Top 51-200
    SMALL_CAP = "small_cap"  # Below 200
    STABLECOIN = "stablecoin"
    DEFI = "defi"
    NFT = "nft"


@dataclass
class CoinConfig:
    """Configuration for a tracked cryptocurrency"""
    coin_id: str  # CoinGecko ID
    symbol: str
    name: str
    category: CoinCategory
    track_detailed_metrics: bool = True
    track_historical: bool = True
    priority: int = 1  # 1=highest, 5=lowest


@dataclass
class CryptoStreamConfig:
    """Main cryptocurrency tracking configuration"""
    
    # Project Metadata
    PROJECT_NAME: str = "CryptoStream"
    SOURCE_SYSTEM: str = "CoinGecko"
    VERSION: str = "1.0.0"
    
    # Target Currency
    TARGET_CURRENCY: CryptoCurrency = CryptoCurrency.USD
    
    # Tracked Coins Configuration
    TRACKED_COINS: List[CoinConfig] = field(default_factory=list)
    
    # Data Collection Settings
    BATCH_SIZE: int = 100  # Records per API call
    COLLECTION_FREQUENCY_MINUTES: int = 5
    ENABLE_REAL_TIME: bool = False
    
    # Storage Configuration
    ENABLE_RAW_STORAGE: bool = True  # Store raw API responses
    COMPRESSION_ENABLED: bool = True
    RETENTION_DAYS: int = 365
    
    # Feature Flags
    ENABLE_PRICE_ALERTS: bool = False
    ENABLE_ANOMALY_DETECTION: bool = False
    ENABLE_TREND_ANALYSIS: bool = True
    
    def __post_init__(self):
        if not self.TRACKED_COINS:
            self.TRACKED_COINS = self._get_default_coins()
    
    def _get_default_coins(self) -> List[CoinConfig]:
        """Get default list of tracked cryptocurrencies"""
        return [
            # Major Cryptocurrencies (Top 10)
            CoinConfig(
                coin_id="bitcoin",
                symbol="btc",
                name="Bitcoin",
                category=CoinCategory.MAJOR,
                priority=1
            ),
            CoinConfig(
                coin_id="ethereum",
                symbol="eth",
                name="Ethereum",
                category=CoinCategory.MAJOR,
                priority=1
            ),
            CoinConfig(
                coin_id="binancecoin",
                symbol="bnb",
                name="BNB",
                category=CoinCategory.MAJOR,
                priority=2
            ),
            CoinConfig(
                coin_id="solana",
                symbol="sol",
                name="Solana",
                category=CoinCategory.MAJOR,
                priority=1
            ),
            CoinConfig(
                coin_id="ripple",
                symbol="xrp",
                name="XRP",
                category=CoinCategory.MAJOR,
                priority=2
            ),
            CoinConfig(
                coin_id="cardano",
                symbol="ada",
                name="Cardano",
                category=CoinCategory.MAJOR,
                priority=2
            ),
            CoinConfig(
                coin_id="dogecoin",
                symbol="doge",
                name="Dogecoin",
                category=CoinCategory.MAJOR,
                priority=3
            ),
            CoinConfig(
                coin_id="tron",
                symbol="trx",
                name="TRON",
                category=CoinCategory.LARGE_CAP,
                priority=3
            ),
            CoinConfig(
                coin_id="polkadot",
                symbol="dot",
                name="Polkadot",
                category=CoinCategory.LARGE_CAP,
                priority=3
            ),
            CoinConfig(
                coin_id="avalanche-2",
                symbol="avax",
                name="Avalanche",
                category=CoinCategory.LARGE_CAP,
                priority=3
            ),
            # Stablecoins
            CoinConfig(
                coin_id="tether",
                symbol="usdt",
                name="Tether",
                category=CoinCategory.STABLECOIN,
                priority=2,
                track_detailed_metrics=False
            ),
            CoinConfig(
                coin_id="usd-coin",
                symbol="usdc",
                name="USD Coin",
                category=CoinCategory.STABLECOIN,
                priority=2,
                track_detailed_metrics=False
            ),
        ]
    
    def get_coin_ids(self) -> List[str]:
        """Get list of coin IDs for API calls"""
        return [coin.coin_id for coin in self.TRACKED_COINS]
    
    def get_coin_ids_by_priority(self, max_priority: int = 2) -> List[str]:
        """Get coin IDs filtered by priority"""
        return [
            coin.coin_id 
            for coin in self.TRACKED_COINS 
            if coin.priority <= max_priority
        ]
    
    def get_coins_by_category(self, category: CoinCategory) -> List[CoinConfig]:
        """Get coins filtered by category"""
        return [
            coin 
            for coin in self.TRACKED_COINS 
            if coin.category == category
        ]


# ============================================================================
# Data Quality Configuration
# ============================================================================

@dataclass
class DataQualityConfig:
    """Data quality and validation rules"""
    
    # Completeness Rules
    REQUIRED_FIELDS: List[str] = field(default_factory=lambda: [
        "id", "symbol", "name", "current_price", 
        "market_cap", "total_volume", "last_updated"
    ])
    
    # Validity Rules
    MIN_PRICE: float = 0.0
    MAX_PRICE: float = 1_000_000_000.0  # 1 billion
    MIN_MARKET_CAP: float = 0.0
    MAX_MARKET_CAP: float = 10_000_000_000_000.0  # 10 trillion
    
    # Freshness Rules
    MAX_DATA_AGE_MINUTES: int = 15
    
    # Anomaly Detection
    PRICE_CHANGE_THRESHOLD_PCT: float = 50.0  # Flag if price changes > 50%
    VOLUME_SPIKE_THRESHOLD: float = 5.0  # Flag if volume spikes 5x
    
    # Duplicate Detection
    DUPLICATE_WINDOW_MINUTES: int = 5


# ============================================================================
# Pipeline Configuration
# ============================================================================

@dataclass
class PipelineConfig:
    """Pipeline execution configuration"""
    
    # Catalog and Schema Names
    CATALOG_NAME: str = "main"
    BRONZE_SCHEMA: str = "crypto_bronze"
    SILVER_SCHEMA: str = "crypto_silver"
    GOLD_SCHEMA: str = "crypto_gold"
    
    # Table Names
    BRONZE_TABLE: str = "coingecko_market_data"
    SILVER_TABLE: str = "crypto_market"
    GOLD_SNAPSHOT_TABLE: str = "market_snapshot"
    GOLD_TRENDS_TABLE: str = "price_trends"
    GOLD_MOVERS_TABLE: str = "top_movers"
    GOLD_METRICS_TABLE: str = "market_metrics"
    
    # Processing Configuration
    ENABLE_INCREMENTAL: bool = True
    ENABLE_MERGE: bool = True  # Use MERGE instead of overwrite
    OPTIMIZE_TABLES: bool = True
    Z_ORDER_COLUMNS: List[str] = field(default_factory=lambda: ["coin_id", "ingestion_date"])
    
    # Partition Configuration
    PARTITION_BY: List[str] = field(default_factory=lambda: ["ingestion_date"])
    
    # Monitoring
    TRACK_PIPELINE_METRICS: bool = True
    ALERT_ON_FAILURE: bool = True
    METRICS_TABLE: str = "pipeline_metrics"


# ============================================================================
# Instance Creation Functions
# ============================================================================

def get_crypto_config() -> CryptoStreamConfig:
    """Get cryptocurrency configuration instance"""
    return CryptoStreamConfig()

def get_data_quality_config() -> DataQualityConfig:
    """Get data quality configuration instance"""
    return DataQualityConfig()

def get_pipeline_config() -> PipelineConfig:
    """Get pipeline configuration instance"""
    return PipelineConfig()


# ============================================================================
# Quick Access Lists
# ============================================================================

# Simple list of coin IDs (for backward compatibility)
COINS = [
    "bitcoin",
    "ethereum",
    "binancecoin",
    "solana",
    "ripple",
    "cardano",
    "dogecoin",
    "polkadot",
    "avalanche-2",
    "tron"
]

# Target currency
TARGET_CURRENCY = "usd"