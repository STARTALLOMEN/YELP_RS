"""
Bronze Layer Ingestion using PySpark.
Reads raw JSON and writes to Delta Table (Bronze).
"""
import os
import sys

# Add project root to path so we can import 'processing'
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from processing.common.spark_utils import get_spark_session
from pyspark.sql.functions import input_file_name, current_timestamp
from dotenv import load_dotenv
import logging

# Load env
load_dotenv()
logger = logging.getLogger(__name__)

def ingest_table(spark, source_path: str, delta_path: str, table_name: str):
    """
    Generic function to ingest a single JSON file into Delta.
    """
    if not os.path.exists(source_path):
        logger.warning(f"Source file not found: {source_path}. Skipping {table_name}.")
        return

    logger.info(f"[{table_name}] Reading JSON from {source_path}...")
    
    # Read JSON
    # Yelp JSON is line-delimited
    df = spark.read.json(source_path)
    
    # Add metadata
    df = df.withColumn("_ingest_timestamp", current_timestamp()) \
           .withColumn("_source_file", input_file_name())
    
    logger.info(f"[{table_name}] Writing to {delta_path}...")
    
    # Write to Delta
    # Mode = Overwrite for full refresh (safest for Bronze init)
    # Mode = Append for streaming
    # Here we use 'overwrite' to ensure a clean state
    df.write.format("delta").mode("overwrite").save(delta_path)
    
    logger.info(f"[{table_name}] Done. Count: {df.count()}")

def main():
    spark = get_spark_session(app_name="Yelp_Bronze_Ingest")
    
    # 1. Config Paths
    bronze_source = os.getenv("BRONZE_SOURCE_PATH")
    if not bronze_source:
        logger.error("BRONZE_SOURCE_PATH not set.")
        sys.exit(1)
        
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    # Use absolute path for Delta Table
    # Note: On Windows, Spark path handling can be tricky with "C:". 
    # Best to use forward slashes or strict os.path.join
    bronze_delta_root = os.path.join(project_root, "data", "bronze_delta")
    
    # 2. Ingest Business
    business_src = os.path.join(bronze_source, "yelp_academic_dataset_business.json")
    business_dst = os.path.join(bronze_delta_root, "business")
    ingest_table(spark, business_src, business_dst, "Business")

    # 3. Ingest User
    user_src = os.path.join(bronze_source, "yelp_academic_dataset_user.json")
    user_dst = os.path.join(bronze_delta_root, "user")
    ingest_table(spark, user_src, user_dst, "User")
    
    # Review is large, uncomment if ready
    # review_src = os.path.join(bronze_source, "yelp_academic_dataset_review.json")
    # review_dst = os.path.join(bronze_delta_root, "review")
    # ingest_table(spark, review_src, review_dst, "Review")
    
    logger.info("All Ingestion Jobs Completed.")
    spark.stop()

if __name__ == "__main__":
    main()
