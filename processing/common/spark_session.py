"""Shared Spark session & utility helpers with standardized table naming."""
from __future__ import annotations
import os
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from delta import *

logger = logging.getLogger(__name__)

APP_NAME = os.getenv("APP_NAME", "YELP_RS Processing")
DELTA_VERSION = os.getenv("DELTA_VERSION", "io.delta:delta-core_2.12:3.1.0")

DEFAULT_CONFIGS = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.databricks.delta.optimizeWrite.enabled": "true",
    "spark.databricks.delta.autoCompact.enabled": "true",
    "spark.sql.shuffle.partitions": os.getenv("SPARK_SHUFFLE_PARTITIONS", "200"),
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.sql.adaptive.localShuffleReader.enabled": "true"
}


def get_spark(app_name: str = None, extra_configs: dict | None = None) -> SparkSession:
    """Get or create Spark session with standard configurations"""
    final_app_name = app_name or APP_NAME
    
    builder = (SparkSession.builder
               .appName(final_app_name)
               .config("spark.jars.packages", DELTA_VERSION)
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    
    # Apply defaults
    for k, v in DEFAULT_CONFIGS.items():
        builder = builder.config(k, v)
        
    # Apply custom configs
    if extra_configs:
        for k, v in extra_configs.items():
            builder = builder.config(k, v)
            
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    logger.info(f"Spark session created: {final_app_name}")
    return spark


def create_database_if_not_exists(spark: SparkSession, database_name: str):
    """Create database/schema if it doesn't exist"""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
    logger.info(f"Database '{database_name}' created or already exists")


def setup_standard_databases(spark: SparkSession):
    """Setup standard bronze, silver, gold databases"""
    for db in ["bronze", "silver", "gold"]:
        create_database_if_not_exists(spark, db)


def optimize_table(spark: SparkSession, path: str, z_order_cols: list = None):
    """Optimize Delta table with optional Z-ORDER"""
    logger.info(f"Optimizing table at: {path}")
    
    # Basic OPTIMIZE (open-source Delta doesn't support OPTIMIZE command)
    # Simulate with repartition + overwrite
    df = spark.read.format("delta").load(path)
    optimized_df = df.coalesce(max(1, df.rdd.getNumPartitions() // 4))
    
    optimized_df.write.format("delta") \
                     .mode("overwrite") \
                     .option("mergeSchema", "true") \
                     .save(path)
    
    logger.info(f"Table optimized: {path}")


def vacuum_table(spark: SparkSession, path: str, retention_hours: int = 168):
    """Vacuum Delta table to remove old files (7 days default)"""
    try:
        from delta.tables import DeltaTable
        if DeltaTable.isDeltaTable(spark, path):
            DeltaTable.forPath(spark, path).vacuum(retention_hours)
            logger.info(f"Table vacuumed: {path} (retention: {retention_hours}h)")
        else:
            logger.warning(f"Path is not a Delta table: {path}")
    except Exception as e:
        logger.warning(f"Vacuum failed for {path}: {e}")


def analyze_table_partitions(spark: SparkSession, table_path: str) -> dict:
    """
    Analyze partition distribution for a Delta table
    
    Args:
        spark: SparkSession
        table_path: Path to the Delta table
        
    Returns:
        Dictionary with partition analysis results
    """
    try:
        logger.info(f"Analyzing partitions for table at {table_path}")
        
        # Read table
        df = spark.read.format("delta").load(table_path)
        
        # Get table statistics
        total_records = df.count()
        
        # Try to get partition information from Delta metadata
        try:
            history_df = spark.sql(f"DESCRIBE HISTORY delta.`{table_path}`")
            operations = history_df.select("operation").distinct().collect()
            operation_types = [row.operation for row in operations]
        except:
            operation_types = []
        
        # Basic partition analysis (would need Delta APIs for complete analysis)
        analysis = {
            'table_path': table_path,
            'total_records': total_records,
            'recent_operations': operation_types,
            'analysis_timestamp': datetime.now().isoformat()
        }
        
        logger.info(f"Partition analysis completed for {table_path}")
        return analysis
        
    except Exception as e:
        logger.error(f"Partition analysis failed for {table_path}: {str(e)}")
        raise


def repartition_table_by_columns(spark: SparkSession, table_path: str, 
                                partition_cols: list, num_partitions: int = None):
    """
    Repartition a Delta table by specified columns
    
    Args:
        spark: SparkSession  
        table_path: Path to the Delta table
        partition_cols: List of columns to partition by
        num_partitions: Optional number of partitions
    """
    try:
        logger.info(f"Repartitioning table at {table_path} by columns: {partition_cols}")
        
        # Read current data
        df = spark.read.format("delta").load(table_path)
        
        # Repartition by columns
        if num_partitions:
            repartitioned_df = df.repartition(num_partitions, *partition_cols)
        else:
            repartitioned_df = df.repartition(*partition_cols)
        
        # Write back with new partitioning
        repartitioned_df.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy(*partition_cols) \
            .save(table_path)
        
        logger.info(f"Table repartitioned successfully by {partition_cols}")
        
    except Exception as e:
        logger.error(f"Repartitioning failed for {table_path}: {str(e)}")
        raise
