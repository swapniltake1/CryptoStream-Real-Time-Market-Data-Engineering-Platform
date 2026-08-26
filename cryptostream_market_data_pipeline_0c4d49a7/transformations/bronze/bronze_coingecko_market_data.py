from pyspark import pipelines as dp
from pyspark.sql.functions import *

# Path to external ingestion data folder containing CoinGecko JSON files
DATA_PATH = "/Workspace/Users/swapniltake1@outlook.com/CryptoStream-Real-Time-Market-Data-Engineering-Platform/external_ingestion/data/"


@dp.table(
    name="bronze_coingecko_market_data",
    comment="Bronze layer: raw cryptocurrency market data ingested from CoinGecko JSON files via Auto Loader",
)
def bronze_coingecko_market_data():
    """
    Reads multiLine JSON files from the external ingestion data folder using Auto Loader.
    Each JSON file contains a 'metadata' object and a 'data' array of crypto records.
    Explodes the data array and extracts metadata fields alongside each record.
    """
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("multiLine", "true")
        .load(DATA_PATH)
        .select(
            col("metadata.batch_id").alias("batch_id"),
            col("metadata.ingestion_timestamp").alias("ext_ingestion_ts"),
            col("metadata.source_system").alias("source_system"),
            col("metadata.source_endpoint").alias("source_endpoint"),
            col("metadata.target_currency").alias("target_currency"),
            explode(col("data")).alias("record"),
        )
        .select(
            col("record.*"),
            col("batch_id"),
            to_timestamp(col("ext_ingestion_ts")).alias("external_ingestion_timestamp"),
            to_timestamp(col("ext_ingestion_ts")).alias("ingestion_timestamp"),
            current_timestamp().alias("bronze_ingestion_timestamp"),
            current_timestamp().alias("processing_timestamp"),
            current_date().alias("ingestion_date"),
            col("source_system"),
            col("source_endpoint"),
            col("target_currency"),
            input_file_name().alias("source_file"),
            sha2(
                concat_ws("||",
                    col("id"),
                    col("symbol"),
                    col("name"),
                    col("current_price"),
                    col("market_cap"),
                    col("last_updated"),
                ),
                256,
            ).alias("record_hash"),
        )
    )