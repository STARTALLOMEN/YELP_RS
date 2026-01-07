"""
Base Pipeline Framework for YELP_RS Data Processing
Provides abstract classes for standardized ETL pipeline stages: ingest -> clean -> enrich -> publish
"""
from __future__ import annotations
import os
import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *

logger = logging.getLogger(__name__)


class PipelineStage(ABC):
    """Abstract base class for pipeline stages"""
    
    def __init__(self, 
                 spark: SparkSession,
                 config: Dict[str, Any],
                 stage_name: str,
                 batch_id: Optional[str] = None):
        self.spark = spark
        self.config = config
        self.stage_name = stage_name
        self.batch_id = batch_id or os.getenv("BATCH_ID", f"manual_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        self.source_name = config.get("source_name", "yelp_raw")
        
    @abstractmethod
    def execute(self, input_data: Optional[DataFrame] = None) -> DataFrame:
        """Execute the pipeline stage"""
        pass
        
    def add_metadata_columns(self, df: DataFrame) -> DataFrame:
        """Add standard metadata columns"""
        return df.withColumn("_ingest_ts", current_timestamp()) \
                 .withColumn("_batch_id", lit(self.batch_id)) \
                 .withColumn("_source", lit(self.source_name))
                 
    def log_stage_metrics(self, df: DataFrame, stage_name: str):
        """Log basic metrics for the stage"""
        count = df.count()
        logger.info(f"[{stage_name}] Processed {count:,} records in batch {self.batch_id}")


class IngestStage(PipelineStage):
    """Abstract class for data ingestion stage"""
    
    @abstractmethod
    def ingest(self) -> DataFrame:
        """Load raw data from source"""
        pass
        
    def execute(self, input_data: Optional[DataFrame] = None) -> DataFrame:
        logger.info(f"Starting ingest stage: {self.stage_name}")
        df = self.ingest()
        df_with_meta = self.add_metadata_columns(df)
        self.log_stage_metrics(df_with_meta, "INGEST")
        return df_with_meta


class CleanStage(PipelineStage):
    """Abstract class for data cleaning stage"""
    
    @abstractmethod
    def validate_schema(self, df: DataFrame) -> DataFrame:
        """Validate and fix schema issues"""
        pass
        
    @abstractmethod
    def remove_duplicates(self, df: DataFrame) -> DataFrame:
        """Remove duplicate records"""
        pass
        
    @abstractmethod
    def handle_nulls(self, df: DataFrame) -> DataFrame:
        """Handle null values"""
        pass
        
    def execute(self, input_data: DataFrame) -> DataFrame:
        logger.info(f"Starting clean stage: {self.stage_name}")
        df = self.validate_schema(input_data)
        df = self.remove_duplicates(df)
        df = self.handle_nulls(df)
        self.log_stage_metrics(df, "CLEAN")
        return df


class EnrichStage(PipelineStage):
    """Abstract class for data enrichment stage"""
    
    @abstractmethod
    def feature_engineering(self, df: DataFrame) -> DataFrame:
        """Create new features"""
        pass
        
    def execute(self, input_data: DataFrame) -> DataFrame:
        logger.info(f"Starting enrich stage: {self.stage_name}")
        df = self.feature_engineering(input_data)
        self.log_stage_metrics(df, "ENRICH")
        return df


class PublishStage(PipelineStage):
    """Abstract class for data publishing stage"""
    
    def __init__(self, 
                 spark: SparkSession,
                 config: Dict[str, Any],
                 stage_name: str,
                 output_path: str,
                 write_mode: str = "append",
                 merge_keys: Optional[List[str]] = None,
                 batch_id: Optional[str] = None):
        super().__init__(spark, config, stage_name, batch_id)
        self.output_path = output_path
        self.write_mode = write_mode
        self.merge_keys = merge_keys or []
        
    def execute(self, input_data: DataFrame) -> DataFrame:
        logger.info(f"Starting publish stage: {self.stage_name} -> {self.output_path}")
        self.publish(input_data)
        self.log_stage_metrics(input_data, "PUBLISH")
        return input_data
        
    def publish(self, df: DataFrame):
        """Publish data to target location"""
        if self.write_mode == "merge" and self.merge_keys:
            self._merge_into_delta(df)
        else:
            df.write \
              .format("delta") \
              .mode(self.write_mode) \
              .save(self.output_path)
              
    def _merge_into_delta(self, df: DataFrame):
        """Perform delta merge operation"""
        from delta.tables import DeltaTable
        
        if DeltaTable.isDeltaTable(self.spark, self.output_path):
            target = DeltaTable.forPath(self.spark, self.output_path)
            merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in self.merge_keys])
            
            target.alias("target") \
                  .merge(df.alias("source"), merge_condition) \
                  .whenMatchedUpdateAll() \
                  .whenNotMatchedInsertAll() \
                  .execute()
        else:
            # First time - just write
            df.write.format("delta").mode("overwrite").save(self.output_path)


class StandardPipeline:
    """Standard ETL Pipeline orchestrator"""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        self.spark = spark
        self.config = config
        self.stages: List[PipelineStage] = []
        
    def add_stage(self, stage: PipelineStage) -> 'StandardPipeline':
        """Add a stage to the pipeline"""
        self.stages.append(stage)
        return self
        
    def execute(self) -> DataFrame:
        """Execute all stages in sequence"""
        logger.info(f"Starting pipeline execution with {len(self.stages)} stages")
        
        current_data = None
        for i, stage in enumerate(self.stages):
            logger.info(f"Executing stage {i+1}/{len(self.stages)}: {stage.stage_name}")
            current_data = stage.execute(current_data)
            
        logger.info("Pipeline execution completed successfully")
        return current_data


class TableNamingHelper:
    """Helper class for standardized table naming"""
    
    BRONZE_PREFIX = "bronze"
    SILVER_PREFIX = "silver"
    GOLD_PREFIX = "gold"
    
    @staticmethod
    def bronze_table(table_name: str) -> str:
        """Generate bronze table name"""
        return f"{TableNamingHelper.BRONZE_PREFIX}.{table_name}"
        
    @staticmethod
    def silver_table(table_name: str) -> str:
        """Generate silver table name"""
        return f"{TableNamingHelper.SILVER_PREFIX}.{table_name}"
        
    @staticmethod
    def gold_table(table_name: str) -> str:
        """Generate gold table name"""
        return f"{TableNamingHelper.GOLD_PREFIX}.{table_name}"
        
    @staticmethod
    def get_layer_path(base_path: str, layer: str, table_name: str) -> str:
        """Generate full path for table in specific layer"""
        return f"{base_path.rstrip('/')}/{layer}/{table_name}"


def create_standard_config() -> Dict[str, Any]:
    """Create standard configuration from environment variables"""
    return {
        "app_env": os.getenv("APP_ENV", "dev"),
        "batch_id": os.getenv("BATCH_ID", f"manual_{datetime.now().strftime('%Y%m%d_%H%M%S')}"),
        "source_name": os.getenv("SOURCE_NAME", "yelp_raw"),
        "base_path": os.getenv("DELTA_BASE_PATH", "./data"),
        "bronze_path": os.getenv("BRONZE_BASE_PATH", "./data/bronze"),
        "silver_path": os.getenv("SILVER_BASE_PATH", "./data/silver"),
        "gold_path": os.getenv("GOLD_BASE_PATH", "./data/gold"),
    }
