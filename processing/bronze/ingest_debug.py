import os
import sys
import pandas as pd
from deltalake import write_deltalake
from dotenv import load_dotenv
import logging

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def ingest_debug():
    # 1. Path Setup
    bronze_source = os.getenv("BRONZE_SOURCE_PATH")
    if not bronze_source:
        print("ERROR: Env var not set")
        return

    json_file = os.path.join(bronze_source, "yelp_academic_dataset_business.json")
    
    # Calculate target path relative to THIS script
    # d:\Project\YELP_RS\processing\bronze\ingest_debug.py
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(current_dir)) # Up to YELP_RS
    target_dir = os.path.join(project_root, "data", "bronze_delta", "business")
    
    print(f"Reading from: {json_file}")
    print(f"Writing to:   {target_dir}")
    
    # 2. Read
    if not os.path.exists(json_file):
        print("Source file missing!")
        return

    try:
        # Read only 100 rows to verify
        df = pd.read_json(json_file, lines=True, nrows=100)
        print(f"DataFrame loaded. Shape: {df.shape}")
        print("Columns:", df.columns.tolist())
        
        # 3. Write
        os.makedirs(target_dir, exist_ok=True)
        write_deltalake(target_dir, df, mode="overwrite")
        print("Write Deltalake success.")
        
    except Exception as e:
        print(f"FATAL ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    ingest_debug()
