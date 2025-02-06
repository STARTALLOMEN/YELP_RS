from kafka import KafkaConsumer
import json
from pyspark.sql import SparkSession
from delta import *

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'yelp_business_data'

def create_spark_session():
    return SparkSession.builder \
        .appName("Yelp Business Consumer") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:3.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

def process_message(message):
    # Process and transform the message as needed
    business_data = json.loads(message)
    return business_data

def save_to_delta(spark, data_batch, batch_id):
    if data_batch:
        df = spark.createDataFrame(data_batch)
        
        # Write to Delta table
        df.write.format("delta") \
            .mode("append") \
            .save("delta_lake/bronze/yelp_streaming_business")

def start_consumer():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda x: x.decode('utf-8'),
        auto_offset_reset='earliest',
        group_id='yelp_business_group'
    )
    
    spark = create_spark_session()
    batch_size = 100
    current_batch = []
    batch_id = 0
    
    try:
        for message in consumer:
            processed_data = process_message(message.value)
            current_batch.append(processed_data)
            
            if len(current_batch) >= batch_size:
                save_to_delta(spark, current_batch, batch_id)
                current_batch = []
                batch_id += 1
                
    except KeyboardInterrupt:
        if current_batch:
            save_to_delta(spark, current_batch, batch_id)
        consumer.close()

if __name__ == "__main__":
    start_consumer()
