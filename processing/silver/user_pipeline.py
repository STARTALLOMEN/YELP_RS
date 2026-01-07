"""
User Data Pipeline - Standardized Implementation
Follows the new pipeline framework: ingest -> clean -> enrich -> publish  
Uses standardized naming convention: bronze.user, silver.user, gold.user_metrics
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


class UserIngestStage(IngestStage):
    """Ingest stage for user data"""
    
    def ingest(self) -> DataFrame:
        """Load user data from JSON source"""
        source_path = self.config.get("source_path") or os.getenv("BRONZE_USER_PATH")
        if not source_path:
            raise ValueError("User source path not configured")
            
        # Define schema for user data
        schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("review_count", IntegerType(), True),
            StructField("yelping_since", StringType(), True),
            StructField("useful", IntegerType(), True),
            StructField("funny", IntegerType(), True),
            StructField("cool", IntegerType(), True),
            StructField("elite", StringType(), True),
            StructField("friends", StringType(), True),
            StructField("fans", IntegerType(), True),
            StructField("average_stars", DoubleType(), True),
            StructField("compliment_hot", IntegerType(), True),
            StructField("compliment_more", IntegerType(), True),
            StructField("compliment_profile", IntegerType(), True),
            StructField("compliment_cute", IntegerType(), True),
            StructField("compliment_list", IntegerType(), True),
            StructField("compliment_note", IntegerType(), True),
            StructField("compliment_plain", IntegerType(), True),
            StructField("compliment_cool", IntegerType(), True),
            StructField("compliment_funny", IntegerType(), True),
            StructField("compliment_writer", IntegerType(), True),
            StructField("compliment_photos", IntegerType(), True)
        ])
        
        return self.spark.read.json(source_path, schema=schema)


class UserCleanStage(CleanStage):
    """Clean stage for user data"""
    
    def validate_schema(self, df: DataFrame) -> DataFrame:
        """Validate and clean user data schema"""
        # Convert yelping_since to timestamp
        df = df.withColumn("yelping_since", 
                          to_timestamp(col("yelping_since"), "yyyy-MM-dd HH:mm:ss"))
        
        # Extract year from yelping_since for analysis
        df = df.withColumn("yelping_since_year", year(col("yelping_since")))
        
        # Validate average_stars range
        df = df.withColumn("average_stars",
                          when(col("average_stars").between(0, 5), col("average_stars"))
                          .otherwise(lit(None)))
        
        return df
    
    def remove_duplicates(self, df: DataFrame) -> DataFrame:
        """Remove duplicate user records"""
        initial_count = df.count()
        df_deduped = df.dropDuplicates(["user_id"])
        final_count = df_deduped.count()
        
        if initial_count != final_count:
            logger.warning(f"Removed {initial_count - final_count} duplicate user records")
            
        return df_deduped
    
    def handle_nulls(self, df: DataFrame) -> DataFrame:
        """Handle null values in user data"""
        # Filter out records with missing required fields
        df = df.filter(col("user_id").isNotNull())
        
        # Fill numerical fields with 0
        numerical_cols = [
            "review_count", "useful", "funny", "cool", "fans",
            "compliment_hot", "compliment_more", "compliment_profile",
            "compliment_cute", "compliment_list", "compliment_note",
            "compliment_plain", "compliment_cool", "compliment_funny",
            "compliment_writer", "compliment_photos"
        ]
        
        fill_dict = {col: 0 for col in numerical_cols}
        df = df.fillna(fill_dict)
        
        # Fill string fields with empty string
        df = df.fillna({"elite": "", "friends": ""})
        
        return df


class UserEnrichStage(EnrichStage):
    """Enrich stage for user data"""
    
    def feature_engineering(self, df: DataFrame) -> DataFrame:
        """Create enriched user features"""
        logger.info("Starting user feature engineering")
        
        # Parse elite years from elite string
        df = df.withColumn("elite_years_count",
            when(length(col("elite")) > 0,
                 size(split(col("elite"), ","))
            ).otherwise(0)
        )
        
        # Count number of friends
        df = df.withColumn("friends_count",
            when(length(col("friends")) > 0,
                 size(split(col("friends"), ","))
            ).otherwise(0)
        )
        
        # Total votes received (useful + funny + cool)
        df = df.withColumn("total_votes",
                          col("useful") + col("funny") + col("cool"))
        
        # Total compliments received
        compliment_cols = [
            "compliment_hot", "compliment_more", "compliment_profile",
            "compliment_cute", "compliment_list", "compliment_note",
            "compliment_plain", "compliment_cool", "compliment_funny",
            "compliment_writer", "compliment_photos"
        ]
        df = df.withColumn("total_compliments",
                          sum(col(c) for c in compliment_cols))
        
        # User tenure in years
        df = df.withColumn("tenure_years",
            round((datediff(current_date(), col("yelping_since")) / 365.25), 2)
        )
        
        # Activity level categorization
        df = df.withColumn("activity_level",
            when(col("review_count") >= 100, "Very Active")
            .when(col("review_count") >= 50, "Active")
            .when(col("review_count") >= 10, "Moderate")
            .when(col("review_count") >= 1, "Casual")
            .otherwise("Inactive")
        )
        
        # Elite status indicator
        df = df.withColumn("is_elite",
                          col("elite_years_count") > 0)
        
        # Social connectivity level
        df = df.withColumn("social_level",
            when(col("friends_count") >= 100, "Highly Social")
            .when(col("friends_count") >= 50, "Social")
            .when(col("friends_count") >= 10, "Moderately Social")
            .when(col("friends_count") >= 1, "Low Social")
            .otherwise("Not Social")
        )
        
        # Influence score (combination of reviews, votes, compliments, fans)
        df = df.withColumn("influence_score",
            (col("review_count") * 0.4 +
             col("total_votes") * 0.3 +
             col("total_compliments") * 0.2 +
             col("fans") * 0.1) / 10.0
        )
        
        # User tier based on influence
        df = df.withColumn("user_tier",
            when(col("influence_score") >= 50, "Super User")
            .when(col("influence_score") >= 20, "Power User")
            .when(col("influence_score") >= 5, "Regular User")
            .when(col("influence_score") >= 1, "Casual User")
            .otherwise("New User")
        )
        
        # Average reviews per year
        df = df.withColumn("reviews_per_year",
            when(col("tenure_years") > 0,
                 round(col("review_count") / col("tenure_years"), 2)
            ).otherwise(0)
        )
        
        # Compliment to review ratio
        df = df.withColumn("compliment_per_review",
            when(col("review_count") > 0,
                 round(col("total_compliments") / col("review_count"), 2)
            ).otherwise(0)
        )
        
        logger.info("User feature engineering completed")
        return df


class UserPipeline:
    """Complete User data pipeline"""
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or create_standard_config()
        self.spark = get_spark(
            app_name="User Pipeline",
            extra_configs={"spark.sql.shuffle.partitions": "100"}
        )
        
    def run_bronze_to_silver(self):
        """Run Bronze to Silver pipeline for user data"""
        logger.info("Starting User Bronze -> Silver pipeline")
        
        # Table paths using standardized naming
        silver_path = TableNamingHelper.get_layer_path(
            self.config["base_path"], "silver", "user"
        )
        
        # Create pipeline
        pipeline = StandardPipeline(self.spark, self.config)
        
        # Add stages
        pipeline.add_stage(
            UserIngestStage(self.spark, self.config, "user_ingest")
        ).add_stage(
            UserCleanStage(self.spark, self.config, "user_clean")
        ).add_stage(
            UserEnrichStage(self.spark, self.config, "user_enrich")
        ).add_stage(
            PublishStage(
                self.spark, self.config, "user_publish_silver",
                output_path=silver_path,
                write_mode="overwrite",  # Users typically full refresh
                partition_cols=["user_tier"]
            )
        )
        
        # Execute pipeline
        result_df = pipeline.execute()
        
        logger.info(f"User Bronze -> Silver pipeline completed. "
                   f"Records processed: {result_df.count():,}")
        
        return result_df
    
    def run_silver_to_gold(self):
        """Run Silver to Gold aggregations for user data"""
        logger.info("Starting User Silver -> Gold pipeline")
        
        silver_path = TableNamingHelper.get_layer_path(
            self.config["base_path"], "silver", "user"
        )
        gold_user_metrics_path = TableNamingHelper.get_layer_path(
            self.config["base_path"], "gold", "user_metrics"
        )
        gold_user_cohorts_path = TableNamingHelper.get_layer_path(
            self.config["base_path"], "gold", "user_cohorts"
        )
        
        # Read silver data
        user_df = self.spark.read.format("delta").load(silver_path)
        
        # User metrics by tier and activity level
        user_metrics = user_df.groupBy("user_tier", "activity_level", "is_elite") \
            .agg(
                count("*").alias("user_count"),
                avg("review_count").alias("avg_reviews"),
                avg("total_votes").alias("avg_votes"),
                avg("total_compliments").alias("avg_compliments"),
                avg("fans").alias("avg_fans"),
                avg("friends_count").alias("avg_friends"),
                avg("tenure_years").alias("avg_tenure"),
                avg("influence_score").alias("avg_influence"),
                avg("average_stars").alias("avg_rating_given"),
                max("influence_score").alias("max_influence"),
                min("influence_score").alias("min_influence")
            ) \
            .withColumn("_ingest_ts", current_timestamp()) \
            .withColumn("_batch_id", lit(self.config["batch_id"])) \
            .withColumn("_source", lit(self.config["source_name"]))
        
        # User cohorts by registration year
        user_cohorts = user_df.groupBy("yelping_since_year") \
            .agg(
                count("*").alias("cohort_size"),
                avg("review_count").alias("avg_reviews_per_user"),
                avg("tenure_years").alias("avg_current_tenure"),
                avg("reviews_per_year").alias("avg_reviews_per_year"),
                countDistinct(when(col("is_elite"), col("user_id"))).alias("elite_users"),
                avg("influence_score").alias("avg_influence_score"),
                percentile_approx("review_count", 0.5).alias("median_reviews"),
                percentile_approx("total_votes", 0.5).alias("median_votes")
            ) \
            .withColumn("elite_percentage",
                       round((col("elite_users") / col("cohort_size")) * 100, 2)) \
            .withColumn("_ingest_ts", current_timestamp()) \
            .withColumn("_batch_id", lit(self.config["batch_id"])) \
            .withColumn("_source", lit(self.config["source_name"]))
        
        # Write to gold layer
        user_metrics.write.format("delta").mode("overwrite").save(gold_user_metrics_path)
        user_cohorts.write.format("delta").mode("overwrite").save(gold_user_cohorts_path)
        
        logger.info("User Silver -> Gold pipeline completed")
        return user_metrics, user_cohorts


def main():
    """Main execution function"""
    logging.basicConfig(level=logging.INFO)
    
    try:
        # Initialize pipeline
        pipeline = UserPipeline()
        
        # Run Bronze -> Silver
        pipeline.run_bronze_to_silver()
        
        # Run Silver -> Gold
        pipeline.run_silver_to_gold()
        
        logger.info("User pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"User pipeline failed: {e}")
        raise
    finally:
        # Clean up
        if 'pipeline' in locals() and pipeline.spark:
            pipeline.spark.stop()


if __name__ == "__main__":
    main()
