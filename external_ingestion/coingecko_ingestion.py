import requests
import json
import os
import uuid
from datetime import datetime, timezone


# ============================================================
# CryptoStream - CoinGecko External API Ingestion
# ============================================================

PROJECT_NAME = "CryptoStream"
SOURCE_SYSTEM = "CoinGecko"

API_URL = "https://api.coingecko.com/api/v3/coins/markets"

TARGET_CURRENCY = "usd"

COINS = [
    "bitcoin",
    "ethereum",
    "solana",
    "cardano",
    "ripple",
    "dogecoin"
]


# ============================================================
# PROJECT DIRECTORIES
# ============================================================

# Location of this Python file
SCRIPT_DIR = os.path.dirname(
    os.path.abspath(__file__)
)

# Project root
PROJECT_DIR = os.path.dirname(
    SCRIPT_DIR
)

# Path 1:
# CryptoStream/external_ingestion/data
EXTERNAL_DATA_DIR = os.path.join(
    SCRIPT_DIR,
    "data"
)

# Path 2:
# CryptoStream/data
PROJECT_DATA_DIR = os.path.join(
    PROJECT_DIR,
    "data"
)


# Create both directories if they don't exist
os.makedirs(
    EXTERNAL_DATA_DIR,
    exist_ok=True
)

os.makedirs(
    PROJECT_DATA_DIR,
    exist_ok=True
)


# ============================================================
# BATCH ID
# ============================================================

def generate_batch_id():

    timestamp = datetime.now(
        timezone.utc
    ).strftime("%Y%m%d%H%M%S")

    unique_id = str(
        uuid.uuid4()
    )[:8]

    return f"BATCH_{timestamp}_{unique_id}"


# ============================================================
# API INGESTION
# ============================================================

def fetch_market_data():

    params = {
        "vs_currency": TARGET_CURRENCY,
        "ids": ",".join(COINS),
        "order": "market_cap_desc",
        "per_page": 100,
        "page": 1,
        "sparkline": "false"
    }

    print("\n" + "=" * 60)
    print("CryptoStream - CoinGecko API Ingestion")
    print("=" * 60)

    print(f"Source       : {SOURCE_SYSTEM}")
    print(f"Coins        : {len(COINS)}")
    print(f"Currency     : {TARGET_CURRENCY}")
    print(f"API          : {API_URL}")

    print("\nCalling CoinGecko API...")

    response = requests.get(
        API_URL,
        params=params,
        timeout=30
    )

    response.raise_for_status()

    return response.json()


# ============================================================
# CREATE BRONZE PAYLOAD
# ============================================================

def create_bronze_payload(data):

    ingestion_timestamp = datetime.now(
        timezone.utc
    ).isoformat()

    batch_id = generate_batch_id()

    payload = {

        "metadata": {

            "project_name": PROJECT_NAME,

            "source_system": SOURCE_SYSTEM,

            "source_endpoint": API_URL,

            "target_currency": TARGET_CURRENCY,

            "ingestion_timestamp":
                ingestion_timestamp,

            "batch_id":
                batch_id,

            "record_count":
                len(data)

        },

        "data": data
    }

    return payload


# ============================================================
# SAVE JSON TO BOTH LOCATIONS
# ============================================================

def save_json_to_both_locations(payload):

    timestamp = datetime.now(
        timezone.utc
    ).strftime("%Y%m%d_%H%M%S")

    filename = (
        f"coingecko_market_{timestamp}.json"
    )

    # --------------------------------------------------------
    # PATH 1
    # external_ingestion/data
    # --------------------------------------------------------

    external_filepath = os.path.join(
        EXTERNAL_DATA_DIR,
        filename
    )

    with open(
        external_filepath,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            payload,
            file,
            indent=4
        )


    # --------------------------------------------------------
    # PATH 2
    # project-level data
    # --------------------------------------------------------

    project_filepath = os.path.join(
        PROJECT_DATA_DIR,
        filename
    )

    with open(
        project_filepath,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            payload,
            file,
            indent=4
        )


    return (
        external_filepath,
        project_filepath
    )


# ============================================================
# MAIN
# ============================================================

def main():

    try:

        # ----------------------------------------------------
        # STEP 1 - Fetch API data
        # ----------------------------------------------------

        data = fetch_market_data()

        print(
            f"\nRecords received: {len(data)}"
        )


        # ----------------------------------------------------
        # STEP 2 - Create enterprise metadata
        # ----------------------------------------------------

        payload = create_bronze_payload(
            data
        )


        # ----------------------------------------------------
        # STEP 3 - Save to both locations
        # ----------------------------------------------------

        (
            external_filepath,
            project_filepath
        ) = save_json_to_both_locations(
            payload
        )


        # ----------------------------------------------------
        # STEP 4 - Success information
        # ----------------------------------------------------

        print("\n" + "=" * 60)
        print("INGESTION COMPLETED SUCCESSFULLY")
        print("=" * 60)

        print(
            f"\nRecords Written : "
            f"{payload['metadata']['record_count']}"
        )

        print(
            f"Batch ID        : "
            f"{payload['metadata']['batch_id']}"
        )

        print(
            f"Ingestion Time  : "
            f"{payload['metadata']['ingestion_timestamp']}"
        )

        print("\nFiles created:")

        print(
            f"\n1. External Ingestion:"
        )

        print(
            f"   {external_filepath}"
        )

        print(
            f"\n2. Project Data:"
        )

        print(
            f"   {project_filepath}"
        )

        print("\n" + "=" * 60)


    except requests.exceptions.Timeout:

        print(
            "\nERROR: CoinGecko API request timed out."
        )


    except requests.exceptions.ConnectionError:

        print(
            "\nERROR: Unable to connect to CoinGecko."
        )


    except requests.exceptions.HTTPError as error:

        print(
            f"\nERROR: CoinGecko API HTTP error: "
            f"{error}"
        )


    except Exception as error:

        print(
            f"\nERROR: Unexpected error: "
            f"{error}"
        )


# ============================================================
# SCRIPT ENTRY POINT
# ============================================================

if __name__ == "__main__":

    main()