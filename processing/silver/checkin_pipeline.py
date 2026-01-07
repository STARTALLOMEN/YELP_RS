"""
Checkin Data Pipeline - Standardized Implementation
Follows the new pipeline framework: ingest -> clean -> enrich -> publish
Uses standardized naming convention: bronze.checkin, silver.checkin, gold.checkin_metrics
"""
from __future__ import annotations
import os
import logging
from typing import Dict, Any, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *

from processing.common.base_pipeline import (
    IngestStage, CleanStage, EnrichStage, PublishStage,
    StandardPipeline, TableNamingHelper, create_standard_config
)
from processing.common.spark_session import get_spark

logger = logging.getLogger(__name__)


class CheckinIngestStage(IngestStage):
    """Ingest stage for checkin data"""
    
    def ingest(self) -> DataFrame:
        """Load checkin data from JSON source"""
        source_path = self.config.get("source_path") or os.getenv("BRONZE_CHECKIN_PATH")
        if not source_path:
            raise ValueError("Checkin source path not configured")
            
        # Define schema for checkin data
        schema = StructType([
            StructField("business_id", StringType(), True),
            StructField("date", StringType(), True)
        ])
        
        return self.spark.read.json(source_path, schema=schema)


class CheckinCleanStage(CleanStage):
    """Clean stage for checkin data"""
    
    def validate_schema(self, df: DataFrame) -> DataFrame:
        """Validate and clean checkin data schema"""
        # The date field contains comma-separated datetime strings
        # We need to explode them into individual rows
        
        # Split the date string by comma and explode into separate rows
        df = df.withColumn("date_array", split(col("date"), ", ")) \
               .withColumn("individual_date", explode(col("date_array"))) \
               .drop("date", "date_array") \
               .withColumnRenamed("individual_date", "checkin_date")
        
        # Convert to timestamp
        df = df.withColumn("checkin_datetime",
                          to_timestamp(col("checkin_date"), "yyyy-MM-dd HH:mm:ss"))
        
        # Extract date components for analysis
        df = df.withColumn("checkin_date_only", to_date(col("checkin_datetime"))) \
               .withColumn("year", year(col("checkin_datetime"))) \
               .withColumn("month", month(col("checkin_datetime"))) \
               .withColumn("day", dayofmonth(col("checkin_datetime"))) \
               .withColumn("hour", hour(col("checkin_datetime"))) \
               .withColumn("day_of_week", dayofweek(col("checkin_datetime"))) \
               .withColumn("day_of_year", dayofyear(col("checkin_datetime")))
        
        return df
    
    def remove_duplicates(self, df: DataFrame) -> DataFrame:
        """Remove duplicate checkin records"""
        initial_count = df.count()
        # Remove duplicates based on business_id and exact datetime
        df_deduped = df.dropDuplicates(["business_id", "checkin_datetime"])
        final_count = df_deduped.count()
        
        if initial_count != final_count:
            logger.warning(f"Removed {initial_count - final_count} duplicate checkin records")
            
        return df_deduped
    
    def handle_nulls(self, df: DataFrame) -> DataFrame:
        """Handle null values in checkin data"""
        # Filter out records with missing required fields
        df = df.filter(
            col("business_id").isNotNull() &
            col("checkin_datetime").isNotNull()
        )
        
        return df


class CheckinEnrichStage(EnrichStage):
    """Enrich stage for checkin data"""
    
    def feature_engineering(self, df: DataFrame) -> DataFrame:
        """Create enriched checkin features"""
        logger.info("Starting checkin feature engineering")
        
        # Time-based features
        df = df.withColumn("is_weekend",
                          col("day_of_week").isin([1, 7]))  # Sunday=1, Saturday=7
        
        df = df.withColumn("is_holiday_season",
                          col("month").isin([11, 12]))  # Nov-Dec holiday season
        
        # Time of day categories
        df = df.withColumn("time_of_day",
            when(col("hour").between(6, 11), "Morning")
            .when(col("hour").between(12, 17), "Afternoon")
            .when(col("hour").between(18, 22), "Evening")
            .otherwise("Night")
        )
        
        # Season categorization
        df = df.withColumn("season",
            when(col("month").isin([12, 1, 2]), "Winter")
            .when(col("month").isin([3, 4, 5]), "Spring")
            .when(col("month").isin([6, 7, 8]), "Summer")
            .otherwise("Fall")
        )
        
        # Business hours categorization
        df = df.withColumn("business_hours_category",
            when(col("hour").between(9, 17), "Business Hours")
            .when(col("hour").between(18, 23), "Evening Hours")
            .when(col("hour").between(0, 5), "Late Night")
            .otherwise("Early Morning")
        )
        
        # Peak time indicator (common busy hours)
        df = df.withColumn("is_peak_time",
            (col("hour").between(12, 14)) |  # Lunch time
            (col("hour").between(18, 20))    # Dinner time
        )
        
        # Checkin frequency indicator (days since epoch for sorting)
        df = df.withColumn("days_since_epoch",
                          datediff(col("checkin_date_only"), lit("1970-01-01")))
        
        # Add unique checkin ID for tracking
        df = df.withColumn("checkin_id",
                          concat(col("business_id"), 
                                lit("_"),
                                date_format(col("checkin_datetime"), "yyyyMMddHHmmss")))
        
        logger.info("Checkin feature engineering completed")
        return df


class CheckinPipeline:
    """Complete Checkin data pipeline"""
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or create_standard_config()
        self.spark = get_spark(
            app_name="Checkin Pipeline",
            extra_configs={"spark.sql.shuffle.partitions": "50"}
        )
        
    def run_bronze_to_silver(self):
        """Run Bronze to Silver pipeline for checkin data"""
        logger.info("Starting Checkin Bronze -> Silver pipeline")
        
        # Table paths using standardized naming
        silver_path = TableNamingHelper.get_layer_path(
            self.config["base_path"], "silver", "checkin"
        )
        
        # Create pipeline
        pipeline = StandardPipeline(self.spark, self.config)
        
        # Add stages
        pipeline.add_stage(
            CheckinIngestStage(self.spark, self.config, "checkin_ingest")
        ).add_stage(
            CheckinCleanStage(self.spark, self.config, "checkin_clean")
        ).add_stage(
            CheckinEnrichStage(self.spark, self.config, "checkin_enrich")
        ).add_stage(
            PublishStage(
                self.spark, self.config, "checkin_publish_silver",
                output_path=silver_path,
                write_mode="append",  # Checkins are typically appended
                partition_cols=["year", "month"]
            )
        )
        
        # Execute pipeline
        result_df = pipeline.execute()
        
        logger.info(f"Checkin Bronze -> Silver pipeline completed. "
                   f"Records processed: {result_df.count():,}")
        
        return result_df
    
    def run_silver_to_gold(self):
        """Run Silver to Gold aggregations for checkin data"""
        logger.info("Starting Checkin Silver -> Gold pipeline")
        
        silver_path = TableNamingHelper.get_layer_path(
            self.config["base_path"], "silver", "checkin"
        )
        gold_checkin_metrics_path = TableNamingHelper.get_layer_path(
            self.config["base_path"], "gold", "checkin_metrics"
        )
        gold_business_traffic_path = TableNamingHelper.get_layer_path(
            self.config["base_path"], "gold", "business_traffic_patterns"
        )
        gold_temporal_patterns_path = TableNamingHelper.get_layer_path(
            self.config["base_path"], "gold", "temporal_checkin_patterns"
        )
        
        # Read silver data
        checkin_df = self.spark.read.format("delta").load(silver_path)
        
        # Daily checkin metrics
        daily_metrics = checkin_df.groupBy("checkin_date_only", "day_of_week", "is_weekend") \
            .agg(
                count("*").alias("total_checkins"),
                countDistinct("business_id").alias("unique_businesses"),
                avg("hour").alias("avg_checkin_hour"),
                countDistinct(when(col("is_peak_time"), col("checkin_id"))).alias("peak_time_checkins")
            ) \
            .withColumn("peak_time_percentage",
                       round((col("peak_time_checkins") / col("total_checkins")) * 100, 2)) \
            .withColumn("_ingest_ts", current_timestamp()) \
            .withColumn("_batch_id", lit(self.config["batch_id"])) \
            .withColumn("_source", lit(self.config["source_name"]))
        
        # Business traffic patterns
        business_traffic = checkin_df.groupBy("business_id") \
            .agg(
                count("*").alias("total_checkins"),
                countDistinct("checkin_date_only").alias("active_days"),
                min("checkin_datetime").alias("first_checkin"),
                max("checkin_datetime").alias("last_checkin"),
                avg("hour").alias("avg_checkin_hour"),
                mode("time_of_day").alias("peak_time_of_day"),
                mode("day_of_week").alias("busiest_day_of_week"),
                countDistinct(when(col("is_weekend"), col("checkin_id"))).alias("weekend_checkins"),
                countDistinct(when(col("is_peak_time"), col("checkin_id"))).alias("peak_hour_checkins")
            ) \
            .withColumn("checkin_span_days",
                       datediff(col("last_checkin"), col("first_checkin"))) \
            .withColumn("avg_checkins_per_day",
                       when(col("checkin_span_days") > 0,
                            round(col("total_checkins") / col("checkin_span_days"), 2))
                       .otherwise(col("total_checkins"))) \
            .withColumn("weekend_percentage",
                       round((col("weekend_checkins") / col("total_checkins")) * 100, 2)) \
            .withColumn("peak_hour_percentage",
                       round((col("peak_hour_checkins") / col("total_checkins")) * 100, 2)) \
            .withColumn("_ingest_ts", current_timestamp()) \
            .withColumn("_batch_id", lit(self.config["batch_id"])) \
            .withColumn("_source", lit(self.config["source_name"]))
        
        # Temporal patterns analysis
        temporal_patterns = checkin_df.groupBy("hour", "day_of_week", "time_of_day", "season") \
            .agg(
                count("*").alias("checkin_count"),
                countDistinct("business_id").alias("unique_businesses"),
                countDistinct("checkin_date_only").alias("unique_dates")
            ) \
            .withColumn("avg_checkins_per_business",
                       round(col("checkin_count") / col("unique_businesses"), 2)) \
            .withColumn("_ingest_ts", current_timestamp()) \
            .withColumn("_batch_id", lit(self.config["batch_id"])) \
            .withColumn("_source", lit(self.config["source_name"]))
        
        # Write to gold layer
        daily_metrics.write.format("delta").mode("overwrite").save(gold_checkin_metrics_path)
        business_traffic.write.format("delta").mode("overwrite").save(gold_business_traffic_path)
        temporal_patterns.write.format("delta").mode("overwrite").save(gold_temporal_patterns_path)
        
        logger.info("Checkin Silver -> Gold pipeline completed")
        return daily_metrics, business_traffic, temporal_patterns


def main():
    """Main execution function"""
    logging.basicConfig(level=logging.INFO)
    
    try:
        # Initialize pipeline
        pipeline = CheckinPipeline()
        
        # Run Bronze -> Silver
        pipeline.run_bronze_to_silver()
        
        # Run Silver -> Gold
        pipeline.run_silver_to_gold()
        
        logger.info("Checkin pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"Checkin pipeline failed: {e}")
        raise
    finally:
        # Clean up
        if 'pipeline' in locals() and pipeline.spark:
            pipeline.spark.stop()


if __name__ == "__main__":
    main()
