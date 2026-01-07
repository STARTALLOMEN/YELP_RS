# Streaming (Kafka)
Producers and consumers for ingesting Yelp data streams.

## Files
- `producer.py` : Fetches data from external API and publishes to Kafka topic.
- `consumer.py` : Consumes topic and (planned) writes to Bronze storage.

## Configuration
All sensitive values (bootstrap servers, topics, API keys) are loaded from environment variables.

## Roadmap
- Implement consumer persistence to Bronze (Delta / Parquet).
- Add schema registry & Avro/JSON schema validation.
- Integrate monitoring (lag metrics, dead-letter queue).
- Add idempotency / deduplication strategy.
