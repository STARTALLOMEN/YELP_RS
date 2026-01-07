import os
from kafka import KafkaProducer
import requests
import json
import time
from datetime import datetime
from dotenv import load_dotenv

# Load environment (.env at project root or current dir)
load_dotenv()

# Yelp API Configuration
YELP_API_KEY = os.getenv('YELP_API_KEY')
if not YELP_API_KEY:
    raise RuntimeError("Missing YELP_API_KEY environment variable")
YELP_ENDPOINT = os.getenv('YELP_ENDPOINT', 'https://api.yelp.com/v3/businesses/search')
HEADERS = {'Authorization': f'Bearer {YELP_API_KEY}'}

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'yelp_business_data')
DEFAULT_LOCATIONS = os.getenv('INGEST_LOCATIONS', 'New York,Los Angeles,Chicago,Houston,Phoenix').split(',')
POLL_INTERVAL_SEC = int(os.getenv('POLL_INTERVAL_SEC', '60'))

REQUEST_TIMEOUT = int(os.getenv('REQUEST_TIMEOUT', '20'))
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
BACKOFF_SEC = int(os.getenv('BACKOFF_SEC', '5'))


def create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )


def fetch_yelp_data(location, offset=0):
    params = {
        'location': location,
        'limit': 50,
        'offset': offset
    }
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(
                YELP_ENDPOINT,
                headers=HEADERS,
                params=params,
                timeout=REQUEST_TIMEOUT
            )
            if response.status_code == 200:
                data = response.json().get('businesses', [])
                return data
            elif response.status_code == 429:
                # Rate limit: exponential backoff
                sleep_time = BACKOFF_SEC * attempt
                print(f"Rate limited (429). Sleeping {sleep_time}s (attempt {attempt})...")
                time.sleep(sleep_time)
            else:
                print(f"HTTP {response.status_code}: {response.text}")
                break
        except requests.RequestException as e:
            print(f"Request error (attempt {attempt}): {e}")
            time.sleep(BACKOFF_SEC * attempt)
    return []


def stream_to_kafka():
    producer = create_kafka_producer()
    locations = DEFAULT_LOCATIONS
    offset = 0
    try:
        while True:
            for location in locations:
                businesses = fetch_yelp_data(location.strip(), offset)
                if not businesses:
                    print(f"No data fetched for {location} at offset {offset}")
                for business in businesses:
                    business['ingestion_timestamp'] = datetime.utcnow().isoformat()
                    business['source_location'] = location
                    producer.send(KAFKA_TOPIC, value=business)
                    print(f"Sent business: {business.get('name')}")
                offset = (offset + 50) % 1000
            time.sleep(POLL_INTERVAL_SEC)
    except KeyboardInterrupt:
        print("Stopping stream...")
    finally:
        producer.flush()
        producer.close()


if __name__ == '__main__':
    stream_to_kafka()
