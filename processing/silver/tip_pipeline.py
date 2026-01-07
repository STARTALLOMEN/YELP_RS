"""
Tip Data Pipeline - Standardized Implementation
Follows the new pipeline framework: ingest -> clean -> enrich -> publish
Uses standardized naming convention: bronze.tip, silver.tip, gold.tip_metrics
"""
from __future__ import annotations
import os
import logging
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *

from processing.common.base_pipeline import (
    IngestStage, CleanStage, EnrichStage, PublishStage,
    StandardPipeline, TableNamingHelper, create_standard_config
)
from processing.common.spark_session import get_spark

logger = logging.getLogger(__name__)


class TipIngestStage(IngestStage):
    """Ingest stage for tip data"""
    
    def ingest(self) -> DataFrame:
        """Load tip data from JSON source"""
        source_path = self.config.get("source_path") or os.getenv("BRONZE_TIP_PATH")
        if not source_path:
            raise ValueError("Tip source path not configured")
            
        # Define schema for tip data
        schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("business_id", StringType(), True),
            StructField("text", StringType(), True),
            StructField("date", StringType(), True),
            StructField("compliment_count", IntegerType(), True)
        ])
        
        return self.spark.read.json(source_path, schema=schema)


class TipCleanStage(CleanStage):
    """Clean stage for tip data"""
    
    def validate_schema(self, df: DataFrame) -> DataFrame:
        """Validate and clean tip data schema"""
        # Convert date string to timestamp
        df = df.withColumn("tip_datetime", 
                          to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss"))
        
        # Extract date components for analysis
        df = df.withColumn("tip_date", to_date(col("tip_datetime"))) \
               .withColumn("year", year(col("tip_datetime"))) \
               .withColumn("month", month(col("tip_datetime"))) \
               .withColumn("day", dayofmonth(col("tip_datetime"))) \
               .withColumn("hour", hour(col("tip_datetime"))) \
               .withColumn("day_of_week", dayofweek(col("tip_datetime")))
        
        return df
    
    def remove_duplicates(self, df: DataFrame) -> DataFrame:
        """Remove duplicate tip records"""
        initial_count = df.count()
        # Remove duplicates based on user_id, business_id, and text content
        df_deduped = df.dropDuplicates(["user_id", "business_id", "text", "tip_datetime"])
        final_count = df_deduped.count()
        
        if initial_count != final_count:
            logger.warning(f"Removed {initial_count - final_count} duplicate tip records")
            
        return df_deduped
    
    def handle_nulls(self, df: DataFrame) -> DataFrame:
        """Handle null values in tip data"""
        # Filter out records with missing required fields
        df = df.filter(
            col("user_id").isNotNull() &
            col("business_id").isNotNull() &
            col("text").isNotNull() &
            col("tip_datetime").isNotNull() &
            (length(col("text")) > 0)
        )
        
        # Fill compliment_count with 0 if null
        df = df.fillna({"compliment_count": 0})
        
        return df


class TipEnrichStage(EnrichStage):
    """Enrich stage for tip data"""
    
    def feature_engineering(self, df: DataFrame) -> DataFrame:
        """Create enriched tip features"""
        logger.info("Starting tip feature engineering")
        
        # Text analysis features
        df = df.withColumn("text_length", length(col("text")))
        df = df.withColumn("word_count", size(split(col("text"), " ")))
        
        # Text length categories
        df = df.withColumn("text_length_category",
            when(col("text_length") >= 200, "Long")
            .when(col("text_length") >= 100, "Medium")
            .when(col("text_length") >= 30, "Short")
            .otherwise("Very Short")
        )
        
        # Time-based features
        df = df.withColumn("is_weekend",
                          col("day_of_week").isin([1, 7]))  # Sunday=1, Saturday=7
        
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
        
        # Tip engagement level
        df = df.withColumn("engagement_level",
            when(col("compliment_count") >= 5, "High Engagement")
            .when(col("compliment_count") >= 2, "Medium Engagement")
            .when(col("compliment_count") >= 1, "Low Engagement")
            .otherwise("No Engagement")
        )
        
        # Tip recency (days since tip)
        df = df.withColumn("tip_age_days",
                          datediff(current_date(), col("tip_date")))
        
        # Create unique tip ID
        df = df.withColumn("tip_id",
            concat(
                col("user_id"), lit("_"),
                col("business_id"), lit("_"),
                date_format(col("tip_datetime"), "yyyyMMddHHmmss")
            )
        )
        
        # Simple sentiment proxy based on text characteristics
        # (In real scenario, would use NLP libraries)
        df = df.withColumn("has_exclamation", col("text").contains("!"))
        df = df.withColumn("has_question", col("text").contains("?"))
        df = df.withColumn("is_enthusiastic", 
                          col("has_exclamation") | (col("compliment_count") > 0))
        
        # Tip quality score (combination of length, engagement, and characteristics)
        df = df.withColumn("tip_quality_score",
            (when(col("text_length") > 50, 2).otherwise(1) +
             when(col("compliment_count") > 0, 2).otherwise(0) +
             when(col("is_enthusiastic"), 1).otherwise(0) +
             when(col("word_count") > 10, 1).otherwise(0)
            ) / 6.0
        )
        
        # Tip type classification
        df = df.withColumn("tip_type",
            when(col("text").rlike("(?i)(recommend|suggest|try|best|love)"), "Recommendation")
            .when(col("text").rlike("(?i)(avoid|bad|terrible|worst|don't)"), "Warning")
            .when(col("text").rlike("(?i)(good|great|amazing|excellent|perfect)"), "Praise")
            .when(col("text").rlike("(?i)(hours|open|closed|time|call)"), "Information")
            .otherwise("General")
        )
        
        logger.info("Tip feature engineering completed")
        return df


class TipPipeline:
    """Complete Tip data pipeline"""
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or create_standard_config()
        self.spark = get_spark(
            app_name="Tip Pipeline",
            extra_configs={"spark.sql.shuffle.partitions": "100"}
        )
        
    def run_bronze_to_silver(self):
        """Run Bronze to Silver pipeline for tip data"""
        logger.info("Starting Tip Bronze -> Silver pipeline")
        
        # Table paths using standardized naming
        silver_path = TableNamingHelper.get_layer_path(
            self.config["base_path"], "silver", "tip"
        )
        
        # Create pipeline
        pipeline = StandardPipeline(self.spark, self.config)
        
        # Add stages
        pipeline.add_stage(
            TipIngestStage(self.spark, self.config, "tip_ingest")
        ).add_stage(
            TipCleanStage(self.spark, self.config, "tip_clean")
        ).add_stage(
            TipEnrichStage(self.spark, self.config, "tip_enrich")
        ).add_stage(
            PublishStage(
                self.spark, self.config, "tip_publish_silver",
                output_path=silver_path,
                write_mode="merge",
                merge_keys=["tip_id"],
                partition_cols=["year", "month"]
            )
        )
        
        # Execute pipeline
        result_df = pipeline.execute()
        
        logger.info(f"Tip Bronze -> Silver pipeline completed. "
                   f"Records processed: {result_df.count():,}")
        
        return result_df
    
    def run_silver_to_gold(self):
        """Run Silver to Gold aggregations for tip data"""
        logger.info("Starting Tip Silver -> Gold pipeline")
        
        silver_path = TableNamingHelper.get_layer_path(
            self.config["base_path"], "silver", "tip"
        )
        gold_tip_metrics_path = TableNamingHelper.get_layer_path(
            self.config["base_path"], "gold", "tip_metrics"
        )
        gold_user_tip_stats_path = TableNamingHelper.get_layer_path(
            self.config["base_path"], "gold", "user_tip_stats"
        )
        gold_business_tip_stats_path = TableNamingHelper.get_layer_path(
            self.config["base_path"], "gold", "business_tip_stats"
        )
        gold_tip_content_analysis_path = TableNamingHelper.get_layer_path(
            self.config["base_path"], "gold", "tip_content_analysis"
        )
        
        # Read silver data
        tip_df = self.spark.read.format("delta").load(silver_path)
        
        # Daily tip metrics
        daily_tip_metrics = tip_df.groupBy("tip_date", "day_of_week", "is_weekend") \
            .agg(
                count("*").alias("tip_count"),
                countDistinct("user_id").alias("unique_tippers"),
                countDistinct("business_id").alias("unique_businesses"),
                avg("text_length").alias("avg_text_length"),
                avg("compliment_count").alias("avg_compliments"),
                avg("tip_quality_score").alias("avg_quality_score"),
                sum("compliment_count").alias("total_compliments")
            ) \
            .withColumn("_ingest_ts", current_timestamp()) \
            .withColumn("_batch_id", lit(self.config["batch_id"])) \
            .withColumn("_source", lit(self.config["source_name"]))
        
        # User tip statistics
        user_tip_stats = tip_df.groupBy("user_id") \
            .agg(
                count("*").alias("tip_count"),
                countDistinct("business_id").alias("businesses_tipped"),
                sum("compliment_count").alias("total_compliments_received"),
                avg("text_length").alias("avg_tip_length"),
                avg("tip_quality_score").alias("avg_quality_score"),
                min("tip_datetime").alias("first_tip_date"),
                max("tip_datetime").alias("last_tip_date"),
                mode("tip_type").alias("most_common_tip_type"),
                mode("time_of_day").alias("preferred_tip_time")
            ) \
            .withColumn("tip_span_days",
                       datediff(col("last_tip_date"), col("first_tip_date"))) \
            .withColumn("avg_compliments_per_tip",
                       when(col("tip_count") > 0,
                            round(col("total_compliments_received") / col("tip_count"), 2))
                       .otherwise(0)) \
            .withColumn("_ingest_ts", current_timestamp()) \
            .withColumn("_batch_id", lit(self.config["batch_id"])) \
            .withColumn("_source", lit(self.config["source_name"]))
        
        # Business tip statistics
        business_tip_stats = tip_df.groupBy("business_id") \
            .agg(
                count("*").alias("tip_count"),
                countDistinct("user_id").alias("unique_tippers"),
                sum("compliment_count").alias("total_compliments"),
                avg("text_length").alias("avg_tip_length"),
                avg("tip_quality_score").alias("avg_quality_score"),
                min("tip_datetime").alias("first_tip_date"),
                max("tip_datetime").alias("last_tip_date"),
                mode("tip_type").alias("most_common_tip_type")
            ) \
            .withColumn("tip_span_days",
                       datediff(col("last_tip_date"), col("first_tip_date"))) \
            .withColumn("avg_tips_per_tipper",
                       when(col("unique_tippers") > 0,
                            round(col("tip_count") / col("unique_tippers"), 2))
                       .otherwise(0)) \
            .withColumn("_ingest_ts", current_timestamp()) \
            .withColumn("_batch_id", lit(self.config["batch_id"])) \
            .withColumn("_source", lit(self.config["source_name"]))
        
        # Tip content analysis
        tip_content_analysis = tip_df.groupBy("tip_type", "text_length_category", 
                                              "engagement_level", "time_of_day") \
            .agg(
                count("*").alias("tip_count"),
                avg("text_length").alias("avg_text_length"),
                avg("compliment_count").alias("avg_compliments"),
                avg("tip_quality_score").alias("avg_quality_score"),
                countDistinct("user_id").alias("unique_users"),
                countDistinct("business_id").alias("unique_businesses")
            ) \
            .withColumn("_ingest_ts", current_timestamp()) \
            .withColumn("_batch_id", lit(self.config["batch_id"])) \
            .withColumn("_source", lit(self.config["source_name"]))
        
        # Write to gold layer
        daily_tip_metrics.write.format("delta").mode("overwrite").save(gold_tip_metrics_path)
        user_tip_stats.write.format("delta").mode("overwrite").save(gold_user_tip_stats_path)
        business_tip_stats.write.format("delta").mode("overwrite").save(gold_business_tip_stats_path)
        tip_content_analysis.write.format("delta").mode("overwrite").save(gold_tip_content_analysis_path)
        
        logger.info("Tip Silver -> Gold pipeline completed")
        return daily_tip_metrics, user_tip_stats, business_tip_stats, tip_content_analysis


def main():
    """Main execution function"""
    logging.basicConfig(level=logging.INFO)
    
    try:
        # Initialize pipeline
        pipeline = TipPipeline()
        
        # Run Bronze -> Silver
        pipeline.run_bronze_to_silver()
        
        # Run Silver -> Gold
        pipeline.run_silver_to_gold()
        
        logger.info("Tip pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"Tip pipeline failed: {e}")
        raise
    finally:
        # Clean up
        if 'pipeline' in locals() and pipeline.spark:
            pipeline.spark.stop()


if __name__ == "__main__":
    main()
