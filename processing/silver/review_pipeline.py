"""
Review Data Pipeline - Standardized Implementation  
Follows the new pipeline framework: ingest -> clean -> enrich -> publish
Uses standardized naming convention: bronze.review, silver.review, gold.review_metrics
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


class ReviewIngestStage(IngestStage):
    """Ingest stage for review data"""
    
    def ingest(self) -> DataFrame:
        """Load review data from JSON source"""
        source_path = self.config.get("source_path") or os.getenv("BRONZE_REVIEW_PATH")
        if not source_path:
            raise ValueError("Review source path not configured")
            
        # Define schema for review data
        schema = StructType([
            StructField("review_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("business_id", StringType(), True),
            StructField("stars", DoubleType(), True),
            StructField("useful", IntegerType(), True),
            StructField("funny", IntegerType(), True),
            StructField("cool", IntegerType(), True),
            StructField("text", StringType(), True),
            StructField("date", StringType(), True)
        ])
        
        return self.spark.read.json(source_path, schema=schema)


class ReviewCleanStage(CleanStage):
    """Clean stage for review data"""
    
    def validate_schema(self, df: DataFrame) -> DataFrame:
        """Validate and clean review data schema"""
        # Convert date string to timestamp
        df = df.withColumn("date", to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss"))
        
        # Extract date components
        df = df.withColumn("year", year(col("date"))) \
               .withColumn("month", month(col("date"))) \
               .withColumn("day", dayofmonth(col("date")))
        
        # Validate star ratings
        df = df.filter(col("stars").between(1, 5))
        
        return df
    
    def remove_duplicates(self, df: DataFrame) -> DataFrame:
        """Remove duplicate review records"""
        initial_count = df.count()
        df_deduped = df.dropDuplicates(["review_id"])
        final_count = df_deduped.count()
        
        if initial_count != final_count:
            logger.warning(f"Removed {initial_count - final_count} duplicate review records")
            
        return df_deduped
    
    def handle_nulls(self, df: DataFrame) -> DataFrame:
        """Handle null values in review data"""
        # Filter out records with missing required fields
        df = df.filter(
            col("review_id").isNotNull() &
            col("user_id").isNotNull() &
            col("business_id").isNotNull() &
            col("stars").isNotNull() &
            (length(col("text")) > 0)
        )
        
        # Fill optional fields with defaults
        df = df.fillna({
            "useful": 0,
            "funny": 0,
            "cool": 0
        })
        
        return df


class ReviewEnrichStage(EnrichStage):
    """Enrich stage for review data"""
    
    def feature_engineering(self, df: DataFrame) -> DataFrame:
        """Create enriched review features"""
        logger.info("Starting review feature engineering")
        
        # Text length feature
        df = df.withColumn("text_length", length(col("text")))
        
        # Total engagement (votes)
        df = df.withColumn("total_votes", 
                          col("useful") + col("funny") + col("cool"))
        
        # Rating categories
        df = df.withColumn("rating_category",
            when(col("stars") >= 4.5, "Excellent")
            .when(col("stars") >= 3.5, "Very Good")
            .when(col("stars") >= 2.5, "Average")
            .when(col("stars") >= 1.5, "Below Average")
            .otherwise("Poor")
        )
        
        # Review recency (days since review)
        df = df.withColumn("review_age_days",
                          datediff(current_date(), col("date")))
        
        # Text length categories
        df = df.withColumn("text_length_category",
            when(col("text_length") >= 500, "Long")
            .when(col("text_length") >= 200, "Medium")
            .when(col("text_length") >= 50, "Short")
            .otherwise("Very Short")
        )
        
        # Engagement level
        df = df.withColumn("engagement_level",
            when(col("total_votes") >= 10, "High")
            .when(col("total_votes") >= 3, "Medium")
            .when(col("total_votes") >= 1, "Low")
            .otherwise("None")
        )
        
        # Weekend/weekday review
        df = df.withColumn("is_weekend",
                          dayofweek(col("date")).isin([1, 7]))  # Sunday=1, Saturday=7
        
        # Review sentiment proxy (simple based on stars and engagement)
        df = df.withColumn("sentiment_score",
            (col("stars") - 3.0) * 2.0 + 
            least(col("total_votes") / 10.0, lit(1.0))
        )
        
        logger.info("Review feature engineering completed")
        return df


class ReviewPipeline:
    """Complete Review data pipeline"""
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or create_standard_config()
        self.spark = get_spark(
            app_name="Review Pipeline",
            extra_configs={"spark.sql.shuffle.partitions": "200"}  # Reviews have more data
        )
        
    def run_bronze_to_silver(self):
        """Run Bronze to Silver pipeline for review data"""
        logger.info("Starting Review Bronze -> Silver pipeline")
        
        # Table paths using standardized naming
        silver_path = TableNamingHelper.get_layer_path(
            self.config["base_path"], "silver", "review"
        )
        
        # Create pipeline
        pipeline = StandardPipeline(self.spark, self.config)
        
        # Add stages
        pipeline.add_stage(
            ReviewIngestStage(self.spark, self.config, "review_ingest")
        ).add_stage(
            ReviewCleanStage(self.spark, self.config, "review_clean")
        ).add_stage(
            ReviewEnrichStage(self.spark, self.config, "review_enrich")
        ).add_stage(
            PublishStage(
                self.spark, self.config, "review_publish_silver",
                output_path=silver_path,
                write_mode="merge",
                merge_keys=["review_id"]
            )
        )
        
        # Execute pipeline
        result_df = pipeline.execute()
        
        logger.info(f"Review Bronze -> Silver pipeline completed. "
                   f"Records processed: {result_df.count():,}")
        
        return result_df
    
    def run_silver_to_gold(self):
        """Run Silver to Gold aggregations for review data"""
        logger.info("Starting Review Silver -> Gold pipeline")
        
        silver_path = TableNamingHelper.get_layer_path(
            self.config["base_path"], "silver", "review"
        )
        gold_review_metrics_path = TableNamingHelper.get_layer_path(
            self.config["base_path"], "gold", "review_metrics"
        )
        gold_user_review_stats_path = TableNamingHelper.get_layer_path(
            self.config["base_path"], "gold", "user_review_stats"
        )
        gold_business_review_stats_path = TableNamingHelper.get_layer_path(
            self.config["base_path"], "gold", "business_review_stats"
        )
        
        # Read silver data
        review_df = self.spark.read.format("delta").load(silver_path)
        
        # Daily review metrics
        daily_metrics = review_df.groupBy("year", "month", "day") \
            .agg(
                count("*").alias("review_count"),
                avg("stars").alias("avg_rating"),
                avg("text_length").alias("avg_text_length"),
                sum("total_votes").alias("total_engagement"),
                avg("sentiment_score").alias("avg_sentiment")
            ) \
            .withColumn("date", to_date(concat_ws("-", col("year"), col("month"), col("day")))) \
            .withColumn("_ingest_ts", current_timestamp()) \
            .withColumn("_batch_id", lit(self.config["batch_id"])) \
            .withColumn("_source", lit(self.config["source_name"]))
        
        # User review statistics
        user_stats = review_df.groupBy("user_id") \
            .agg(
                count("*").alias("review_count"),
                avg("stars").alias("avg_rating_given"),
                sum("total_votes").alias("total_votes_received"),
                avg("text_length").alias("avg_text_length"),
                min("date").alias("first_review_date"),
                max("date").alias("last_review_date"),
                avg("sentiment_score").alias("avg_sentiment"),
                countDistinct("business_id").alias("unique_businesses_reviewed")
            ) \
            .withColumn("review_span_days",
                       datediff(col("last_review_date"), col("first_review_date"))) \
            .withColumn("_ingest_ts", current_timestamp()) \
            .withColumn("_batch_id", lit(self.config["batch_id"])) \
            .withColumn("_source", lit(self.config["source_name"]))
        
        # Business review statistics
        business_stats = review_df.groupBy("business_id") \
            .agg(
                count("*").alias("review_count"),
                avg("stars").alias("avg_rating_received"),
                sum("total_votes").alias("total_engagement"),
                avg("text_length").alias("avg_review_length"),
                min("date").alias("first_review_date"),
                max("date").alias("last_review_date"),
                countDistinct("user_id").alias("unique_reviewers"),
                avg("sentiment_score").alias("avg_sentiment_received")
            ) \
            .withColumn("review_span_days",
                       datediff(col("last_review_date"), col("first_review_date"))) \
            .withColumn("_ingest_ts", current_timestamp()) \
            .withColumn("_batch_id", lit(self.config["batch_id"])) \
            .withColumn("_source", lit(self.config["source_name"]))
        
        # Write to gold layer
        daily_metrics.write.format("delta").mode("overwrite").save(gold_review_metrics_path)
        user_stats.write.format("delta").mode("overwrite").save(gold_user_review_stats_path)
        business_stats.write.format("delta").mode("overwrite").save(gold_business_review_stats_path)
        
        logger.info("Review Silver -> Gold pipeline completed")
        return daily_metrics, user_stats, business_stats


def main():
    """Main execution function"""
    logging.basicConfig(level=logging.INFO)
    
    try:
        # Initialize pipeline
        pipeline = ReviewPipeline()
        
        # Run Bronze -> Silver
        pipeline.run_bronze_to_silver()
        
        # Run Silver -> Gold
        pipeline.run_silver_to_gold()
        
        logger.info("Review pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"Review pipeline failed: {e}")
        raise
    finally:
        # Clean up
        if 'pipeline' in locals() and pipeline.spark:
            pipeline.spark.stop()


if __name__ == "__main__":
    main()
