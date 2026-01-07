"""
Business Data Pipeline - Standardized Implementation
Follows the new pipeline framework: ingest -> clean -> enrich -> publish
Uses standardized naming convention: bronze.business, silver.business, gold.business
"""
from __future__ import annotations
import os
import logging
from typing import Dict, Any, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import Imputer

from processing.common.base_pipeline import (
    IngestStage, CleanStage, EnrichStage, PublishStage, 
    StandardPipeline, TableNamingHelper, create_standard_config
)
from processing.common.spark_session import get_spark

logger = logging.getLogger(__name__)


class BusinessIngestStage(IngestStage):
    """Ingest stage for business data"""
    
    def ingest(self) -> DataFrame:
        """Load business data from JSON source"""
        source_path = self.config.get("source_path") or os.getenv("BRONZE_BUSINESS_PATH")
        if not source_path:
            raise ValueError("Business source path not configured")
            
        # Define schema for business data
        schema = StructType([
            StructField("business_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("address", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("postal_code", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("stars", DoubleType(), True),
            StructField("review_count", IntegerType(), True),
            StructField("is_open", IntegerType(), True),
            StructField("attributes", StringType(), True),
            StructField("categories", StringType(), True),
            StructField("hours", StringType(), True)
        ])
        
        return self.spark.read.json(source_path, schema=schema)


class BusinessCleanStage(CleanStage):
    """Clean stage for business data"""
    
    def validate_schema(self, df: DataFrame) -> DataFrame:
        """Validate and clean business data schema"""
        # Ensure required columns exist and have correct types
        df = df.withColumn("business_id", col("business_id").cast(StringType())) \
               .withColumn("name", col("name").cast(StringType())) \
               .withColumn("stars", col("stars").cast(DoubleType())) \
               .withColumn("review_count", col("review_count").cast(IntegerType())) \
               .withColumn("is_open", col("is_open").cast(IntegerType()))
        
        # Filter out records without business_id
        df = df.filter(col("business_id").isNotNull() & (length(col("business_id")) > 0))
        
        return df
    
    def remove_duplicates(self, df: DataFrame) -> DataFrame:
        """Remove duplicate business records"""
        initial_count = df.count()
        df_deduped = df.dropDuplicates(["business_id"])
        final_count = df_deduped.count()
        
        if initial_count != final_count:
            logger.warning(f"Removed {initial_count - final_count} duplicate business records")
            
        return df_deduped
    
    def handle_nulls(self, df: DataFrame) -> DataFrame:
        """Handle null values in business data"""
        # Fill null values with defaults
        df = df.fillna({
            "name": "Unknown Business",
            "address": "Unknown Address",
            "city": "Unknown",
            "state": "Unknown",
            "postal_code": "00000",
            "latitude": 0.0,
            "longitude": 0.0,
            "stars": 0.0,
            "review_count": 0,
            "is_open": 0,
            "attributes": "{}",
            "categories": "",
            "hours": "{}"
        })
        
        return df


class BusinessEnrichStage(EnrichStage):
    """Enrich stage for business data"""
    
    def feature_engineering(self, df: DataFrame) -> DataFrame:
        """Create enriched business features"""
        logger.info("Starting business feature engineering")
        
        # Business performance category
        df = df.withColumn("performance_category",
            when(col("stars") >= 4.5, "Excellent")
            .when(col("stars") >= 4.0, "Very Good")
            .when(col("stars") >= 3.0, "Good")
            .when(col("stars") >= 2.0, "Average")
            .otherwise("Poor")
        )
        
        # Review volume category
        df = df.withColumn("review_volume_category",
            when(col("review_count") >= 1000, "High Volume")
            .when(col("review_count") >= 100, "Medium Volume")
            .when(col("review_count") >= 10, "Low Volume")
            .otherwise("Very Low Volume")
        )
        
        # Business status
        df = df.withColumn("business_status",
            when(col("is_open") == 1, "Active")
            .otherwise("Closed")
        )
        
        # Location features
        df = df.withColumn("has_valid_location",
            when((col("latitude") != 0.0) & (col("longitude") != 0.0), True)
            .otherwise(False)
        )
        
        # Category processing
        df = df.withColumn("category_count",
            when(col("categories").isNotNull() & (col("categories") != ""),
                 size(split(col("categories"), ",")))
            .otherwise(0)
        )
        
        df = df.withColumn("primary_category",
            when(col("categories").isNotNull() & (col("categories") != ""),
                 trim(split(col("categories"), ",")[0]))
            .otherwise("Uncategorized")
        )
        
        # Quality score (composite metric)
        df = df.withColumn("quality_score",
            (col("stars") * 0.7) + 
            (least(col("review_count") / 100.0, lit(5.0)) * 0.3)
        )
        
        logger.info("Business feature engineering completed")
        return df


class BusinessPipeline:
    """Complete Business data pipeline"""
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or create_standard_config()
        self.spark = get_spark(
            app_name="Business Pipeline",
            extra_configs={"spark.sql.shuffle.partitions": "50"}  # Smaller for business data
        )
        
    def run_bronze_to_silver(self):
        """Run Bronze to Silver pipeline for business data"""
        logger.info("Starting Business Bronze -> Silver pipeline")
        
        # Table paths using standardized naming
        bronze_path = TableNamingHelper.get_layer_path(
            self.config["base_path"], "bronze", "business"
        )
        silver_path = TableNamingHelper.get_layer_path(
            self.config["base_path"], "silver", "business"
        )
        
        # Create pipeline
        pipeline = StandardPipeline(self.spark, self.config)
        
        # Add stages
        pipeline.add_stage(
            BusinessIngestStage(self.spark, self.config, "business_ingest")
        ).add_stage(
            BusinessCleanStage(self.spark, self.config, "business_clean")
        ).add_stage(
            BusinessEnrichStage(self.spark, self.config, "business_enrich")
        ).add_stage(
            PublishStage(
                self.spark, self.config, "business_publish_silver",
                output_path=silver_path,
                write_mode="merge",
                merge_keys=["business_id"]
            )
        )
        
        # Execute pipeline
        result_df = pipeline.execute()
        
        logger.info(f"Business Bronze -> Silver pipeline completed. "
                   f"Records processed: {result_df.count():,}")
        
        return result_df
        
    def run_silver_to_gold(self):
        """Run Silver to Gold aggregations for business data"""
        logger.info("Starting Business Silver -> Gold pipeline")
        
        silver_path = TableNamingHelper.get_layer_path(
            self.config["base_path"], "silver", "business"
        )
        gold_business_summary_path = TableNamingHelper.get_layer_path(
            self.config["base_path"], "gold", "business_summary"
        )
        gold_category_stats_path = TableNamingHelper.get_layer_path(
            self.config["base_path"], "gold", "category_stats"
        )
        
        # Read silver data
        business_df = self.spark.read.format("delta").load(silver_path)
        
        # Create business summary aggregations
        business_summary = business_df.groupBy("state", "city", "business_status") \
            .agg(
                count("*").alias("business_count"),
                avg("stars").alias("avg_stars"),
                avg("review_count").alias("avg_review_count"),
                sum("review_count").alias("total_reviews"),
                avg("quality_score").alias("avg_quality_score")
            ) \
            .withColumn("_ingest_ts", current_timestamp()) \
            .withColumn("_batch_id", lit(self.config["batch_id"])) \
            .withColumn("_source", lit(self.config["source_name"]))
        
        # Create category statistics
        category_stats = business_df.filter(col("primary_category") != "Uncategorized") \
            .groupBy("primary_category", "business_status") \
            .agg(
                count("*").alias("business_count"),
                avg("stars").alias("avg_stars"),
                avg("review_count").alias("avg_review_count"),
                percentile_approx("stars", 0.5).alias("median_stars"),
                avg("quality_score").alias("avg_quality_score")
            ) \
            .withColumn("_ingest_ts", current_timestamp()) \
            .withColumn("_batch_id", lit(self.config["batch_id"])) \
            .withColumn("_source", lit(self.config["source_name"]))
        
        # Write to gold layer
        business_summary.write.format("delta") \
                              .mode("overwrite") \
                              .save(gold_business_summary_path)
        
        category_stats.write.format("delta") \
                           .mode("overwrite") \
                           .save(gold_category_stats_path)
        
        logger.info("Business Silver -> Gold pipeline completed")
        return business_summary, category_stats


def main():
    """Main execution function"""
    logging.basicConfig(level=logging.INFO)
    
    try:
        # Initialize pipeline
        pipeline = BusinessPipeline()
        
        # Run Bronze -> Silver
        pipeline.run_bronze_to_silver()
        
        # Run Silver -> Gold  
        pipeline.run_silver_to_gold()
        
        logger.info("Business pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"Business pipeline failed: {e}")
        raise
    finally:
        # Clean up
        if 'pipeline' in locals() and pipeline.spark:
            pipeline.spark.stop()


if __name__ == "__main__":
    main()
