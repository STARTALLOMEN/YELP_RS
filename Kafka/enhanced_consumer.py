"""
Enhanced Kafka Consumer with Structured Streaming
Features:
- Structured Streaming consumer with checkpointing
- Schema registry integration support
- Proper offset management
- Error handling and dead letter queue
- Monitoring and alerting
- Graceful shutdown
"""
import os
import sys
import logging
import json
import signal
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.streaming import StreamingQuery
from delta import *

# Add processing modules to path
sys.path.append(str(Path(__file__).parent.parent))

from processing.common.spark_session import get_spark
from processing.common.config_manager import get_config_manager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'consumer_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class StreamingMetrics:
    """Streaming consumer metrics"""
    records_processed: int = 0
    batches_processed: int = 0
    errors_count: int = 0
    last_batch_timestamp: Optional[datetime] = None
    start_time: datetime = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'records_processed': self.records_processed,
            'batches_processed': self.batches_processed,
            'errors_count': self.errors_count,
            'last_batch_timestamp': self.last_batch_timestamp.isoformat() if self.last_batch_timestamp else None,
            'uptime_seconds': (datetime.now() - self.start_time).total_seconds() if self.start_time else 0
        }


class YelpBusinessStreamSchema:
    """Schema definition for Yelp business streaming data"""
    
    @staticmethod
    def get_kafka_schema() -> StructType:
        """Get schema for Kafka message structure"""
        return StructType([
            StructField("key", StringType(), True),
            StructField("value", StringType(), True),
            StructField("topic", StringType(), True),
            StructField("partition", IntegerType(), True),
            StructField("offset", LongType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("timestampType", IntegerType(), True)
        ])
    
    @staticmethod
    def get_business_schema() -> StructType:
        """Get schema for Yelp business data"""
        return StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("image_url", StringType(), True),
            StructField("is_closed", BooleanType(), True),
            StructField("url", StringType(), True),
            StructField("review_count", IntegerType(), True),
            StructField("rating", DoubleType(), True),
            StructField("phone", StringType(), True),
            StructField("display_phone", StringType(), True),
            StructField("price", StringType(), True),
            StructField("distance", DoubleType(), True),
            StructField("transactions", ArrayType(StringType()), True),
            StructField("categories", ArrayType(StructType([
                StructField("alias", StringType(), True),
                StructField("title", StringType(), True)
            ])), True),
            StructField("coordinates", StructType([
                StructField("latitude", DoubleType(), True),
                StructField("longitude", DoubleType(), True)
            ]), True),
            StructField("location", StructType([
                StructField("address1", StringType(), True),
                StructField("address2", StringType(), True),
                StructField("address3", StringType(), True),
                StructField("city", StringType(), True),
                StructField("zip_code", StringType(), True),
                StructField("country", StringType(), True),
                StructField("state", StringType(), True),
                StructField("display_address", ArrayType(StringType()), True)
            ]), True),
            StructField("_metadata", StructType([
                StructField("ingested_at", StringType(), True),
                StructField("source", StringType(), True),
                StructField("search_location", StringType(), True),
                StructField("producer_id", StringType(), True),
                StructField("batch_id", StringType(), True)
            ]), True)
        ])


class EnhancedKafkaConsumer:
    """Enhanced Kafka consumer with Structured Streaming"""
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or get_config_manager().get_standard_config()
        
        # Kafka configuration
        self.kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.kafka_topic = os.getenv('KAFKA_TOPIC', 'yelp_business_data')
        self.consumer_group = os.getenv('KAFKA_CONSUMER_GROUP', 'yelp_streaming_consumer')
        
        # Streaming configuration
        self.checkpoint_location = os.getenv('CHECKPOINT_LOCATION', 
                                           '/tmp/kafka_streaming_checkpoints')
        self.trigger_interval = os.getenv('TRIGGER_INTERVAL', '30 seconds')
        self.max_files_per_trigger = int(os.getenv('MAX_FILES_PER_TRIGGER', '10'))
        
        # Output configuration
        base_path = self.config['base_path']
        self.bronze_output_path = os.path.join(base_path, 'bronze', 'business_streaming')
        self.error_output_path = os.path.join(base_path, 'errors', 'business_streaming')
        
        # Initialize components
        self.spark = None
        self.streaming_query = None
        self.metrics = StreamingMetrics(start_time=datetime.now())
        self.shutdown_requested = False
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info("Enhanced Kafka Consumer initialized", extra={
            'kafka_servers': self.kafka_bootstrap_servers,
            'topic': self.kafka_topic,
            'checkpoint_location': self.checkpoint_location,
            'bronze_output': self.bronze_output_path
        })
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}, initiating graceful shutdown")
        self.shutdown_requested = True
        if self.streaming_query:
            self.streaming_query.stop()
    
    def _initialize_spark(self) -> SparkSession:
        """Initialize Spark session with streaming configurations"""
        extra_configs = {
            "spark.sql.streaming.checkpointLocation": self.checkpoint_location,
            "spark.sql.streaming.forceDeleteTempCheckpointLocation": "true",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.streaming.stopGracefullyOnShutdown": "true",
        }
        
        return get_spark(
            app_name="Yelp Kafka Streaming Consumer",
            extra_configs=extra_configs
        )
    
    def _create_kafka_stream(self) -> DataFrame:
        """Create Kafka streaming DataFrame"""
        kafka_df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", self.kafka_topic) \
            .option("kafka.group.id", self.consumer_group) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .option("kafka.session.timeout.ms", "30000") \
            .option("kafka.request.timeout.ms", "40000") \
            .option("maxOffsetsPerTrigger", "1000") \
            .load()
        
        logger.info("Kafka streaming DataFrame created")
        return kafka_df
    
    def _parse_kafka_messages(self, kafka_df: DataFrame) -> DataFrame:
        """Parse and validate Kafka messages"""
        
        # Parse JSON from Kafka value
        business_schema = YelpBusinessStreamSchema.get_business_schema()
        
        parsed_df = kafka_df.select(
            col("key").cast("string").alias("message_key"),
            col("value").cast("string").alias("raw_value"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),
            from_json(col("value").cast("string"), business_schema).alias("business_data")
        )
        
        # Add processing metadata
        enriched_df = parsed_df.select(
            "*",
            current_timestamp().alias("processed_at"),
            lit(self.consumer_group).alias("consumer_group"),
            year(col("kafka_timestamp")).alias("year"),
            month(col("kafka_timestamp")).alias("month"),
            dayofmonth(col("kafka_timestamp")).alias("day")
        )
        
        return enriched_df
    
    def _validate_business_data(self, df: DataFrame) -> DataFrame:
        """Validate business data and separate valid/invalid records"""
        
        # Define validation conditions
        valid_conditions = (
            col("business_data.id").isNotNull() &
            col("business_data.name").isNotNull() &
            col("business_data.location").isNotNull() &
            (length(col("business_data.id")) > 0) &
            (length(col("business_data.name")) > 0)
        )
        
        # Add validation flag
        validated_df = df.withColumn("is_valid", valid_conditions)
        
        return validated_df
    
    def _process_batch(self, batch_df: DataFrame, batch_id: int):
        """Process each streaming batch"""
        try:
            logger.info(f"Processing batch {batch_id}")
            
            batch_count = batch_df.count()
            if batch_count == 0:
                logger.info(f"Batch {batch_id} is empty, skipping")
                return
            
            logger.info(f"Batch {batch_id} contains {batch_count} records")
            
            # Separate valid and invalid records
            valid_df = batch_df.filter(col("is_valid") == True)
            invalid_df = batch_df.filter(col("is_valid") == False)
            
            valid_count = valid_df.count()
            invalid_count = invalid_df.count()
            
            logger.info(f"Batch {batch_id}: {valid_count} valid, {invalid_count} invalid records")
            
            # Write valid records to bronze layer
            if valid_count > 0:
                valid_df.write \
                    .format("delta") \
                    .mode("append") \
                    .partitionBy("year", "month") \
                    .save(self.bronze_output_path)
                
                logger.info(f"Batch {batch_id}: Wrote {valid_count} valid records to bronze")
            
            # Write invalid records to error path for investigation
            if invalid_count > 0:
                invalid_df.write \
                    .format("delta") \
                    .mode("append") \
                    .partitionBy("year", "month") \
                    .save(self.error_output_path)
                
                logger.warning(f"Batch {batch_id}: Wrote {invalid_count} invalid records to error path")
            
            # Update metrics
            self.metrics.records_processed += valid_count
            self.metrics.batches_processed += 1
            self.metrics.errors_count += invalid_count
            self.metrics.last_batch_timestamp = datetime.now()
            
            # Log metrics every 10 batches
            if batch_id % 10 == 0:
                metrics_data = self.metrics.to_dict()
                logger.info("Consumer metrics", extra=metrics_data)
                
        except Exception as e:
            logger.error(f"Error processing batch {batch_id}: {str(e)}")
            self.metrics.errors_count += 1
            raise
    
    def _setup_monitoring(self):
        """Setup monitoring and alerting"""
        
        def log_streaming_progress(query: StreamingQuery):
            """Log streaming query progress"""
            try:
                progress = query.lastProgress
                if progress:
                    logger.info("Streaming progress", extra={
                        'batch_id': progress.get('batchId'),
                        'input_rows_per_second': progress.get('inputRowsPerSecond'),
                        'processed_rows_per_second': progress.get('processedRowsPerSecond'),
                        'batch_duration': progress.get('batchDuration'),
                        'num_input_rows': progress.get('numInputRows'),
                        'sources': progress.get('sources', [])
                    })
            except Exception as e:
                logger.warning(f"Error logging streaming progress: {e}")
        
        return log_streaming_progress
    
    def start_streaming(self):
        """Start the streaming consumer"""
        try:
            logger.info("Starting Enhanced Kafka Consumer")
            
            # Initialize Spark
            self.spark = self._initialize_spark()
            
            # Create directories
            os.makedirs(self.bronze_output_path, exist_ok=True)
            os.makedirs(self.error_output_path, exist_ok=True)
            os.makedirs(self.checkpoint_location, exist_ok=True)
            
            # Create Kafka stream
            kafka_df = self._create_kafka_stream()
            
            # Parse and validate messages
            parsed_df = self._parse_kafka_messages(kafka_df)
            validated_df = self._validate_business_data(parsed_df)
            
            # Start streaming query with foreachBatch
            self.streaming_query = validated_df.writeStream \
                .trigger(processingTime=self.trigger_interval) \
                .foreachBatch(self._process_batch) \
                .option("checkpointLocation", self.checkpoint_location) \
                .queryName("yelp_business_consumer") \
                .start()
            
            logger.info("Streaming query started successfully")
            
            # Setup monitoring
            monitor_fn = self._setup_monitoring()
            
            # Wait for termination or shutdown signal
            while not self.shutdown_requested and self.streaming_query.isActive:
                try:
                    # Monitor progress every 30 seconds
                    self.streaming_query.awaitTermination(30)
                    monitor_fn(self.streaming_query)
                    
                except KeyboardInterrupt:
                    logger.info("Received keyboard interrupt")
                    break
            
            logger.info("Streaming query terminated")
            
        except Exception as e:
            logger.error(f"Streaming consumer error: {str(e)}")
            raise
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Clean up resources"""
        logger.info("Cleaning up consumer resources")
        
        try:
            if self.streaming_query and self.streaming_query.isActive:
                logger.info("Stopping streaming query...")
                self.streaming_query.stop()
                logger.info("Streaming query stopped")
            
            # Log final metrics
            final_metrics = self.metrics.to_dict()
            logger.info("Final consumer metrics", extra=final_metrics)
            
            if self.spark:
                self.spark.stop()
                logger.info("Spark session stopped")
                
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")
        
        logger.info("Consumer cleanup complete")


class StreamingHealthChecker:
    """Health checker for streaming applications"""
    
    def __init__(self, consumer: EnhancedKafkaConsumer):
        self.consumer = consumer
    
    def check_health(self) -> Dict[str, Any]:
        """Check streaming application health"""
        health_status = {
            'timestamp': datetime.now().isoformat(),
            'status': 'unknown',
            'checks': {}
        }
        
        try:
            # Check if query is active
            if self.consumer.streaming_query:
                health_status['checks']['query_active'] = self.consumer.streaming_query.isActive
            
            # Check metrics
            metrics = self.consumer.metrics.to_dict()
            health_status['checks']['metrics'] = metrics
            
            # Check for recent activity
            if self.consumer.metrics.last_batch_timestamp:
                time_since_last_batch = (datetime.now() - self.consumer.metrics.last_batch_timestamp).total_seconds()
                health_status['checks']['time_since_last_batch_seconds'] = time_since_last_batch
                health_status['checks']['recent_activity'] = time_since_last_batch < 300  # 5 minutes
            
            # Overall status
            all_checks_pass = all([
                health_status['checks'].get('query_active', False),
                health_status['checks'].get('recent_activity', True)
            ])
            
            health_status['status'] = 'healthy' if all_checks_pass else 'unhealthy'
            
        except Exception as e:
            health_status['status'] = 'error'
            health_status['error'] = str(e)
        
        return health_status


def main():
    """Main execution function"""
    try:
        consumer = EnhancedKafkaConsumer()
        consumer.start_streaming()
    except Exception as e:
        logger.error(f"Consumer failed to start: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
