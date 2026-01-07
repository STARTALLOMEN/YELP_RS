"""Environment variable validation script.
Usage:
  python scripts/check_env.py [--strict]

--strict : fail (exit 1) if any required variable missing, else prints warnings only.

This covers security requirement: script kiểm tra thiếu biến môi trường khi khởi chạy.
"""
from __future__ import annotations
import os
import sys
import json
from pathlib import Path
from dotenv import load_dotenv

# Attempt load from conventional locations
for candidate in [Path('.env'), Path('config/.env')]:
    if candidate.exists():
        load_dotenv(candidate)

# Define required & optional variable groups
REQUIRED = {
    # Azure SQL
    'AZURE_SQL_SERVER',
    'AZURE_SQL_DATABASE',
    'AZURE_SQL_USERNAME',
    'AZURE_SQL_PASSWORD',
    # Yelp / Kafka streaming
    'YELP_API_KEY',
    'KAFKA_BOOTSTRAP_SERVERS',
    'KAFKA_TOPIC',
    # Model artifacts
    'MODEL_PATH',
    'VECTORIZER_PATH'
}

OPTIONAL = {
    'EVENT_HUB_CONNECTION_STRING',
    'EVENT_HUB_NAME',
    'INGEST_LOCATIONS',
    'POLL_INTERVAL_SEC',
    'REQUEST_TIMEOUT',
    'MAX_RETRIES',
    'BACKOFF_SEC',
    'BRONZE_BUSINESS_PATH',
    'SILVER_BUSINESS_PATH',
    'BRONZE_REVIEW_PATH',
    'SILVER_REVIEW_PATH'
}

# Environment tiering
ENVIRONMENT = os.getenv('APP_ENV', 'dev')

missing = sorted([v for v in REQUIRED if not os.getenv(v)])
report = {
    'environment': ENVIRONMENT,
    'required_total': len(REQUIRED),
    'required_missing': missing,
    'optional_missing': sorted([v for v in OPTIONAL if not os.getenv(v)]),
}

print(json.dumps(report, indent=2))

if missing:
    print(f"Missing required environment variables: {', '.join(missing)}", file=sys.stderr)
    if '--strict' in sys.argv:
        sys.exit(1)
    else:
        print("(Non-strict mode) Proceeding with warnings only.")
else:
    print("All required environment variables present.")
