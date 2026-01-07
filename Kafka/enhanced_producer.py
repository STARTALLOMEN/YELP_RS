"""
Enhanced Yelp Data Producer for Kafka
Features:
- Exponential backoff retry mechanism  
- Structured JSON logging
- Rate limiting compliance with Yelp API
- Schema validation
- Error monitoring and alerting
- Graceful shutdown handling
"""
import os
import sys
import json
import time
import logging
import signal
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, asdict
import uuid
from enum import Enum
import threading
import queue

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError
from dotenv import load_dotenv
import jsonschema
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Load environment variables
load_dotenv()

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'producer_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class ProducerStatus(Enum):
    """Producer status enumeration"""
    INITIALIZING = "initializing"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERROR = "error"


@dataclass
class ProducerMetrics:
    """Producer performance metrics"""
    messages_sent: int = 0
    messages_failed: int = 0
    api_requests_made: int = 0
    api_requests_failed: int = 0
    rate_limit_hits: int = 0
    last_successful_fetch: Optional[datetime] = None
    start_time: datetime = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'messages_sent': self.messages_sent,
            'messages_failed': self.messages_failed,
            'api_requests_made': self.api_requests_made,
            'api_requests_failed': self.api_requests_failed,
            'rate_limit_hits': self.rate_limit_hits,
            'last_successful_fetch': self.last_successful_fetch.isoformat() if self.last_successful_fetch else None,
            'uptime_seconds': (datetime.now() - self.start_time).total_seconds() if self.start_time else 0
        }


class YelpBusinessSchema:
    """JSON Schema for Yelp business data validation"""
    
    SCHEMA = {
        "type": "object",
        "required": ["id", "name", "location"],
        "properties": {
            "id": {"type": "string"},
            "name": {"type": "string"},
            "location": {"type": "object"},
            "coordinates": {"type": "object"},
            "phone": {"type": ["string", "null"]},
            "display_phone": {"type": ["string", "null"]},
            "distance": {"type": "number"},
            "categories": {"type": "array"},
            "rating": {"type": "number"},
            "review_count": {"type": "integer"},
            "url": {"type": "string"},
            "image_url": {"type": ["string", "null"]},
            "is_closed": {"type": "boolean"},
            "price": {"type": ["string", "null"]},
            "transactions": {"type": "array"}
        }
    }
    
    @classmethod
    def validate(cls, data: Dict[str, Any]) -> bool:
        """Validate business data against schema"""
        try:
            jsonschema.validate(data, cls.SCHEMA)
            return True
        except jsonschema.ValidationError as e:
            logger.warning(f"Schema validation failed: {e.message}")
            return False


class RateLimiter:
    """Rate limiter for API requests"""
    
    def __init__(self, max_requests: int = 5000, window_seconds: int = 86400):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.requests = []
        self.lock = threading.Lock()
    
    def can_make_request(self) -> bool:
        """Check if we can make a request within rate limits"""
        with self.lock:
            now = datetime.now()
            # Remove old requests outside the window
            self.requests = [req_time for req_time in self.requests 
                           if (now - req_time).total_seconds() < self.window_seconds]
            
            return len(self.requests) < self.max_requests
    
    def record_request(self):
        """Record that a request was made"""
        with self.lock:
            self.requests.append(datetime.now())
    
    def time_until_next_request(self) -> float:
        """Get seconds to wait before next request"""
        if self.can_make_request():
            return 0.0
        
        with self.lock:
            if not self.requests:
                return 0.0
            
            oldest_request = min(self.requests)
            time_to_wait = self.window_seconds - (datetime.now() - oldest_request).total_seconds()
            return max(0.0, time_to_wait / self.max_requests)


class EnhancedYelpProducer:
    """Enhanced Kafka producer for Yelp data with retry logic and monitoring"""
    
    def __init__(self):
        # Load configuration
        self.yelp_api_key = os.getenv('YELP_API_KEY')
        if not self.yelp_api_key:
            raise RuntimeError("Missing YELP_API_KEY environment variable")
        
        self.yelp_endpoint = os.getenv('YELP_ENDPOINT', 'https://api.yelp.com/v3/businesses/search')
        self.kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.kafka_topic = os.getenv('KAFKA_TOPIC', 'yelp_business_data')
        
        # Producer configuration
        self.locations = os.getenv('INGEST_LOCATIONS', 
                                 'New York,Los Angeles,Chicago,Houston,Phoenix,Philadelphia').split(',')
        self.poll_interval = int(os.getenv('POLL_INTERVAL_SEC', '300'))  # 5 minutes default
        self.batch_size = int(os.getenv('BATCH_SIZE', '50'))
        
        # Rate limiting
        daily_limit = int(os.getenv('YELP_DAILY_LIMIT', '5000'))
        self.rate_limiter = RateLimiter(daily_limit, 86400)
        
        # Initialize components
        self.producer = None
        self.status = ProducerStatus.INITIALIZING
        self.metrics = ProducerMetrics(start_time=datetime.now())
        self.shutdown_event = threading.Event()
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info("Enhanced Yelp Producer initialized", extra={
            'locations': self.locations,
            'poll_interval': self.poll_interval,
            'daily_limit': daily_limit
        })
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}, initiating graceful shutdown")
        self.status = ProducerStatus.STOPPING
        self.shutdown_event.set()
    
    def _create_kafka_producer(self) -> KafkaProducer:
        """Create Kafka producer with optimal configuration"""
        config = {
            'bootstrap_servers': self.kafka_bootstrap_servers.split(','),
            'value_serializer': lambda x: json.dumps(x).encode('utf-8'),
            'key_serializer': lambda x: str(x).encode('utf-8'),
            'acks': 'all',  # Wait for all replicas
            'retries': 5,
            'retry_backoff_ms': 1000,
            'batch_size': 16384,
            'linger_ms': 10,
            'compression_type': 'gzip',
            'max_in_flight_requests_per_connection': 1,  # Ensure ordering
        }
        
        return KafkaProducer(**config)
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        retry=retry_if_exception_type((requests.RequestException, requests.Timeout))
    )
    def _fetch_yelp_data(self, location: str, offset: int = 0) -> Optional[Dict[str, Any]]:
        """Fetch data from Yelp API with retry logic"""
        
        # Check rate limits
        if not self.rate_limiter.can_make_request():
            wait_time = self.rate_limiter.time_until_next_request()
            logger.warning(f"Rate limit reached, waiting {wait_time:.2f} seconds")
            time.sleep(wait_time)
        
        headers = {'Authorization': f'Bearer {self.yelp_api_key}'}
        params = {
            'location': location,
            'limit': self.batch_size,
            'offset': offset,
            'sort_by': 'best_match'
        }
        
        request_id = str(uuid.uuid4())[:8]
        
        logger.info("Making Yelp API request", extra={
            'request_id': request_id,
            'location': location,
            'offset': offset,
            'batch_size': self.batch_size
        })
        
        try:
            self.rate_limiter.record_request()
            self.metrics.api_requests_made += 1
            
            response = requests.get(
                self.yelp_endpoint,
                headers=headers,
                params=params,
                timeout=30
            )
            
            response.raise_for_status()
            
            if response.status_code == 429:  # Rate limited
                self.metrics.rate_limit_hits += 1
                retry_after = int(response.headers.get('Retry-After', 60))
                logger.warning(f"Rate limited by Yelp API, waiting {retry_after} seconds")
                time.sleep(retry_after)
                raise requests.RequestException("Rate limited")
            
            data = response.json()
            
            logger.info("Yelp API request successful", extra={
                'request_id': request_id,
                'businesses_returned': len(data.get('businesses', [])),
                'total_available': data.get('total', 0)
            })
            
            self.metrics.last_successful_fetch = datetime.now()
            return data
            
        except requests.RequestException as e:
            self.metrics.api_requests_failed += 1
            logger.error("Yelp API request failed", extra={
                'request_id': request_id,
                'error': str(e),
                'location': location,
                'offset': offset
            })
            raise
    
    def _enrich_business_data(self, business: Dict[str, Any], location: str) -> Dict[str, Any]:
        """Enrich business data with metadata"""
        enriched = business.copy()
        enriched.update({
            '_metadata': {
                'ingested_at': datetime.now().isoformat(),
                'source': 'yelp_api',
                'search_location': location,
                'producer_id': os.getenv('HOSTNAME', 'unknown'),
                'batch_id': str(uuid.uuid4())
            }
        })
        return enriched
    
    def _send_to_kafka(self, data: Dict[str, Any], key: str = None):
        """Send data to Kafka with error handling"""
        try:
            # Validate data schema
            if not YelpBusinessSchema.validate(data):
                logger.warning("Skipping invalid business data", extra={
                    'business_id': data.get('id', 'unknown')
                })
                return
            
            # Send to Kafka
            future = self.producer.send(
                topic=self.kafka_topic,
                value=data,
                key=key or data.get('id', str(uuid.uuid4()))
            )
            
            # Add callback for delivery confirmation
            future.add_callback(self._on_send_success, data)
            future.add_errback(self._on_send_error, data)
            
        except Exception as e:
            logger.error("Failed to send message to Kafka", extra={
                'business_id': data.get('id', 'unknown'),
                'error': str(e)
            })
            self.metrics.messages_failed += 1
    
    def _on_send_success(self, data: Dict[str, Any], metadata):
        """Callback for successful Kafka send"""
        self.metrics.messages_sent += 1
        logger.debug("Message sent successfully", extra={
            'business_id': data.get('id', 'unknown'),
            'partition': metadata.partition,
            'offset': metadata.offset
        })
    
    def _on_send_error(self, data: Dict[str, Any], exception):
        """Callback for failed Kafka send"""
        self.metrics.messages_failed += 1
        logger.error("Message send failed", extra={
            'business_id': data.get('id', 'unknown'),
            'error': str(exception)
        })
    
    def _log_metrics(self):
        """Log current producer metrics"""
        metrics_data = self.metrics.to_dict()
        logger.info("Producer metrics", extra=metrics_data)
    
    def start(self):
        """Start the producer"""
        try:
            logger.info("Starting Enhanced Yelp Producer")
            self.status = ProducerStatus.RUNNING
            
            # Create Kafka producer
            self.producer = self._create_kafka_producer()
            
            while not self.shutdown_event.is_set():
                start_time = time.time()
                
                # Process each location
                for location in self.locations:
                    if self.shutdown_event.is_set():
                        break
                    
                    try:
                        # Fetch data for this location
                        api_data = self._fetch_yelp_data(location)
                        
                        if not api_data or 'businesses' not in api_data:
                            logger.warning(f"No business data for location: {location}")
                            continue
                        
                        # Send each business to Kafka
                        for business in api_data['businesses']:
                            if self.shutdown_event.is_set():
                                break
                            
                            enriched_business = self._enrich_business_data(business, location)
                            self._send_to_kafka(enriched_business)
                        
                        logger.info(f"Processed {len(api_data['businesses'])} businesses for {location}")
                        
                    except Exception as e:
                        logger.error(f"Error processing location {location}: {str(e)}")
                        continue
                
                # Flush producer and log metrics
                if self.producer:
                    self.producer.flush()
                
                self._log_metrics()
                
                # Calculate sleep time
                elapsed = time.time() - start_time
                sleep_time = max(0, self.poll_interval - elapsed)
                
                if sleep_time > 0:
                    logger.info(f"Sleeping for {sleep_time:.2f} seconds until next poll")
                    self.shutdown_event.wait(sleep_time)
                
        except KeyboardInterrupt:
            logger.info("Received interrupt signal")
        except Exception as e:
            logger.error(f"Producer error: {str(e)}")
            self.status = ProducerStatus.ERROR
            raise
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Clean up resources"""
        logger.info("Cleaning up producer resources")
        self.status = ProducerStatus.STOPPED
        
        if self.producer:
            try:
                self.producer.flush(timeout=10)
                self.producer.close()
                logger.info("Kafka producer closed successfully")
            except Exception as e:
                logger.error(f"Error closing Kafka producer: {str(e)}")
        
        # Log final metrics
        self._log_metrics()
        logger.info("Producer shutdown complete")


def main():
    """Main execution function"""
    try:
        producer = EnhancedYelpProducer()
        producer.start()
    except Exception as e:
        logger.error(f"Producer failed to start: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
