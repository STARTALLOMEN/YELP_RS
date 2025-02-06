from kafka import KafkaProducer
import requests
import json
import time
from datetime import datetime

# Yelp API Configuration
YELP_API_KEY = 'YOUR_API_KEY'
YELP_ENDPOINT = 'https://api.yelp.com/v3/businesses/search'
HEADERS = {'Authorization': f'Bearer {YELP_API_KEY}'}

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'yelp_business_data'

def create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

def fetch_yelp_data(location, offset=0):
    params = {
        'location': location,
        'limit': 50,
        'offset': offset
    }
    
    response = requests.get(
        YELP_ENDPOINT,
        headers=HEADERS,
        params=params
    )
    
    if response.status_code == 200:
        return response.json()['businesses']
    return []

def stream_to_kafka():
    producer = create_kafka_producer()
    locations = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']
    offset = 0
    
    try:
        while True:
            for location in locations:
                businesses = fetch_yelp_data(location, offset)
                
                for business in businesses:
                    # Add timestamp and metadata
                    business['ingestion_timestamp'] = datetime.now().isoformat()
                    business['source_location'] = location
                    
                    # Send to Kafka
                    producer.send(
                        KAFKA_TOPIC,
                        value=business
                    )
                    print(f"Sent data for business: {business['name']}")
                
                # Increment offset for pagination
                offset = (offset + 50) % 1000
                
            # Wait before next batch
            time.sleep(60)  # Respect Yelp API rate limits
            
    except KeyboardInterrupt:
        producer.close()

if __name__ == "__main__":
    stream_to_kafka()
