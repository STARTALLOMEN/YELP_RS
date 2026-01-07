"""
Pytest fixtures for API tests.
"""
import pytest
from unittest.mock import patch, MagicMock


@pytest.fixture
def client():
    """
    Create a test client for the FastAPI app.
    Mocks all external dependencies (DB, Event Hub, Models).
    """
    # Mock environment variables before importing app
    with patch.dict('os.environ', {
        'AZURE_SQL_SERVER': 'test-server',
        'AZURE_SQL_DATABASE': 'test-db',
        'AZURE_SQL_USERNAME': 'test-user',
        'AZURE_SQL_PASSWORD': 'test-pass',
        'EVENT_HUB_CONNECTION_STRING': '',
        'MODEL_PATH': 'nonexistent.pkl',
        'VECTORIZER_PATH': 'nonexistent.pkl',
    }):
        # Import after mocking env
        from fastapi.testclient import TestClient
        from api.app import app
        
        yield TestClient(app)
