import requests
from datetime import datetime, timezone
import time

# CoinGecko API configuration
url = "https://api.coingecko.com/api/v3/coins/markets"

params = {
    "vs_currency": "usd",
    "ids": "bitcoin,ethereum,solana,cardano,ripple,dogecoin",
    "order": "market_cap_desc",
    "per_page": 100,
    "page": 1,
    "sparkline": "false"
}

# Retry logic for API calls
max_retries = 3
for attempt in range(max_retries):
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        print(f"✓ Successfully fetched {len(data)} records from CoinGecko API")
        break
    except requests.exceptions.RequestException as e:
        if attempt < max_retries - 1:
            print(f"Attempt {attempt + 1} failed: {str(e)[:100]}. Retrying in 2 seconds...")
            time.sleep(2)
        else:
            print(f"✗ Failed to fetch data after {max_retries} attempts")
            print(f"Error: {e}")
            raise