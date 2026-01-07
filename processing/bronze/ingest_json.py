import os
import sys
import pandas as pd
from deltalake import write_deltalake, DeltaTable
from dotenv import load_dotenv
import logging
import json

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def ingest_json_to_delta_polars(source_path: str, target_path: str, mode: str = "overwrite"):
    """
    Ingests JSON line-delimited files to Delta Table using pure Python (Pandas + deltalake).
    """
    if not os.path.exists(source_path):
        logger.error(f"Source path NOT FOUND: {source_path}")
        return

    logger.info(f"Reading JSON from {source_path}...")
    
    # Read FULL file
    df = pd.read_json(source_path, lines=True)
    
    # Add metadata
    df["_ingest_timestamp"] = pd.Timestamp.now()
    
    logger.info(f"Writing {len(df)} rows to Delta Table at {target_path}...")
    
    write_deltalake(
        target_path,
        df,
        mode=mode,
        schema_mode="merge" if mode == "append" else "overwrite"
    )
    logger.info("Write completed.")

def main():
    # 1. Get Source Path from Env
    bronze_source_dir = os.getenv("BRONZE_SOURCE_PATH")
    if not bronze_source_dir:
        logger.error("BRONZE_SOURCE_PATH is not set in .env")
        sys.exit(1)
        
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    output_bronze_delta_dir = os.path.join(base_dir, "data", "bronze_delta")
    os.makedirs(output_bronze_delta_dir, exist_ok=True)
    
    # 2. Ingest Business Data
    business_json = os.path.join(bronze_source_dir, "yelp_academic_dataset_business.json")
    business_delta = os.path.join(output_bronze_delta_dir, "business")
    
    print(f"Ingesting Business to: {business_delta}")
    ingest_json_to_delta_polars(business_json, business_delta)
    
    logger.info("Ingestion job finished.")

if __name__ == "__main__":
    main()
