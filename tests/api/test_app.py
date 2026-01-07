"""
API Test Suite for Yelp Recommendation API
Tests the /api/v1/ endpoints
"""
import pytest
from unittest.mock import MagicMock, patch


class TestHealthEndpoint:
    """Tests for /api/v1/health endpoint."""
    
    def test_health_check_returns_ok(self, client):
        """Health check should return status ok."""
        response = client.get("/api/v1/health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}
    
    def test_old_health_endpoint_not_found(self, client):
        """Old endpoint /health should not exist."""
        response = client.get("/health")
        assert response.status_code == 404


class TestOptionsEndpoint:
    """Tests for /api/v1/options endpoint."""
    
    @patch('api.app.get_db_connection')
    def test_options_returns_locations_and_categories(self, mock_db, client):
        """Options endpoint should return locations and categories."""
        # Setup mock
        mock_cursor = MagicMock()
        mock_cursor.fetchall.side_effect = [
            [("Santa Barbara, CA",), ("Los Angeles, CA",)],
            [("Mexican",), ("Italian",)]
        ]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_db.return_value = mock_conn
        
        response = client.get("/api/v1/options")
        
        assert response.status_code == 200
        data = response.json()
        assert "locations" in data
        assert "categories" in data
        assert len(data["locations"]) == 2
        assert len(data["categories"]) == 2


class TestRecommendationsEndpoint:
    """Tests for /api/v1/recommendations endpoint."""
    
    def test_recommendations_requires_location(self, client):
        """Recommendations should fail without location."""
        response = client.post("/api/v1/recommendations", json={
            "category": "Mexican"
        })
        assert response.status_code == 422  # Validation error
    
    def test_recommendations_requires_category(self, client):
        """Recommendations should fail without category."""
        response = client.post("/api/v1/recommendations", json={
            "location": "Santa Barbara, CA"
        })
        assert response.status_code == 422  # Validation error
    
    @patch('api.app.get_db_connection')
    @patch('api.app.send_to_event_hub')
    async def test_recommendations_returns_suggestions(
        self, mock_event_hub, mock_db, client
    ):
        """Recommendations should return list of suggestions."""
        # Setup mock
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("b-001", "Taco Palace", 4.5, 120, "Mexican, Tacos", "Santa Barbara", "CA"),
        ]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_db.return_value = mock_conn
        mock_event_hub.return_value = None
        
        response = client.post("/api/v1/recommendations", json={
            "location": "Santa Barbara, CA",
            "category": "Mexican"
        })
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        assert "suggestions" in data
    
    @patch('api.app.get_db_connection')
    @patch('api.app.send_to_event_hub')
    async def test_recommendations_empty_result(
        self, mock_event_hub, mock_db, client
    ):
        """Recommendations should return empty list when no matches."""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_db.return_value = mock_conn
        mock_event_hub.return_value = None
        
        response = client.post("/api/v1/recommendations", json={
            "location": "Unknown City, XX",
            "category": "NonExistent"
        })
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        assert data["suggestions"] == []
