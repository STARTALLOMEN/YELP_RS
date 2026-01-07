"""
Silver Layer: Business Transformation
Flattens attributes and filters open businesses.
"""
import os
import sys
import logging
from pyspark.sql.functions import col, current_timestamp

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from processing.common.spark_utils import get_spark_session
from dotenv import load_dotenv

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def transform_business():
    load_dotenv()
    
    # 1. Init Spark
    spark = get_spark_session(app_name="Yelp_Silver_Business")
    
    # 2. Define Paths
    root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    bronze_path = os.path.join(root_dir, "data", "bronze_delta", "business")
    silver_path = os.path.join(root_dir, "data", "silver_delta", "business")
    
    if not os.path.exists(bronze_path):
        logger.error(f"Bronze path not found: {bronze_path}")
        return

    logger.info(f"Reading from Bronze: {bronze_path}")
    df = spark.read.format("delta").load(bronze_path)
    
    # 3. Filter Open Businesses
    # Assuming is_open: 1 = Open, 0 = Closed
    initial_count = df.count()
    df_open = df.filter(col("is_open") == 1)
    open_count = df_open.count()
    logger.info(f"Filtered Closed Businesses: {initial_count} -> {open_count}")

    # 4. Flatten Attributes
    # Extract key attributes for filtering
    # Note: If 'attributes' is Struct, use dot notation. If Map, use element_at or col("attributes")["Key"]
    # Based on schema inference, it's likely a Struct if keys are consistent, or map if not.
    # We'll use safe extraction.
    
    logger.info("Flattening attributes...")
    
    # Extract raw attributes
    silver_df = df_open.withColumn("BusinessParking", col("attributes.BusinessParking")) \
                       .withColumn("WiFi", col("attributes.WiFi")) \
                       .withColumn("RestaurantsPriceRange2", col("attributes.RestaurantsPriceRange2")) \
                       .withColumn("OutdoorSeating", col("attributes.OutdoorSeating")) \
                       .withColumn("HasTV", col("attributes.HasTV")) \
                       .withColumn("GoodForKids", col("attributes.GoodForKids")) \
                       .withColumn("NoiseLevel", col("attributes.NoiseLevel")) \
                       .withColumn("Ambience", col("attributes.Ambience"))
    
    # Create boolean indicators for filtering
    from pyspark.sql.functions import when, coalesce, lit, lower
    
    # Parking: True if any parking option is available
    silver_df = silver_df.withColumn(
        "has_parking",
        when(
            (col("attributes.BusinessParking.garage") == True) |
            (col("attributes.BusinessParking.street") == True) |
            (col("attributes.BusinessParking.lot") == True) |
            (col("attributes.BusinessParking.validated") == True) |
            (col("attributes.BusinessParking.valet") == True),
            True
        ).otherwise(False)
    )
    
    # WiFi: True if wifi is available (free or paid)
    silver_df = silver_df.withColumn(
        "has_wifi",
        when(
            (lower(col("attributes.WiFi")) == "free") |
            (lower(col("attributes.WiFi")) == "'free'") |
            (lower(col("attributes.WiFi")) == "paid"),
            True
        ).otherwise(False)
    )
    
    # Price Range: Convert to integer (1-4)
    silver_df = silver_df.withColumn(
        "price_range",
        coalesce(col("attributes.RestaurantsPriceRange2").cast("int"), lit(0))
    )
    
    # Outdoor Seating: Boolean
    silver_df = silver_df.withColumn(
        "has_outdoor_seating",
        when(col("attributes.OutdoorSeating") == True, True).otherwise(False)
    )
    
    # Good For Kids: Boolean
    silver_df = silver_df.withColumn(
        "is_good_for_kids",
        when(col("attributes.GoodForKids") == True, True).otherwise(False)
    )
    
    # Reservations: Boolean
    silver_df = silver_df.withColumn(
        "accepts_reservations",
        when(col("attributes.RestaurantsReservations") == True, True).otherwise(False)
    )
    
    # Delivery: Boolean
    silver_df = silver_df.withColumn(
        "has_delivery",
        when(col("attributes.RestaurantsDelivery") == True, True).otherwise(False)
    )
    
    # Takeout: Boolean
    silver_df = silver_df.withColumn(
        "has_takeout",
        when(col("attributes.RestaurantsTakeOut") == True, True).otherwise(False)
    )
    
    logger.info("Created boolean indicators: has_parking, has_wifi, price_range, etc.")
                       
    # Add Metadata
    silver_df = silver_df.withColumn("_silver_timestamp", current_timestamp())

    # 5. Write to Silver
    logger.info(f"Writing to Silver Delta: {silver_path}")
    
    silver_df.write.format("delta") \
             .mode("overwrite") \
             .option("mergeSchema", "true") \
             .save(silver_path)
             
    logger.info("Business Transformation Complete.")
    spark.stop()

if __name__ == "__main__":
    transform_business()
