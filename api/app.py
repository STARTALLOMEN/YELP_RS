"""
FastAPI Application for Yelp Recommendation System
API Version: v1
Port: 8081
"""
from __future__ import annotations

import json
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import List, Optional

import joblib
import pyodbc
import structlog
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, APIRouter
from pydantic import BaseModel, Field

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger(__name__)

# Load environment variables (.env optional)
ENV_PATH = Path(__file__).resolve().parent.parent / '.env'
if ENV_PATH.exists():
    load_dotenv(ENV_PATH)
else:
    load_dotenv()  # fallback if user exports in shell


def get_env(name: str, default: Optional[str] = None, required: bool = False) -> Optional[str]:
    """
    Fetch environment variable with optional default or raise if required.

    Args:
        name: Environment variable name.
        default: Default value if not found.
        required: If True, raises RuntimeError when variable is missing.

    Returns:
        The environment variable value or default.

    Raises:
        RuntimeError: If required is True and variable is not set.
    """
    val = os.getenv(name, default)
    if required and (val is None or val == ''):
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val


# =============================================================================
# Pydantic Models (Request/Response)
# =============================================================================

class HealthResponse(BaseModel):
    """Health check response model."""
    status: str = Field(..., example="ok")


class OptionsResponse(BaseModel):
    """Response model for available locations and categories."""
    locations: List[str] = Field(..., example=["Santa Barbara, CA"])
    categories: List[str] = Field(..., example=["Mexican", "Italian"])


class RecommendationFilters(BaseModel):
    """Optional filters for recommendation request."""
    min_stars: Optional[float] = Field(None, ge=1.0, le=5.0, example=4.0)
    max_price_range: Optional[int] = Field(None, ge=1, le=4, example=2)
    has_parking: Optional[bool] = Field(None, example=True)
    has_wifi: Optional[bool] = Field(None, example=True)
    has_outdoor_seating: Optional[bool] = Field(None, example=False)
    is_good_for_kids: Optional[bool] = Field(None, example=True)
    has_delivery: Optional[bool] = Field(None, example=False)
    has_takeout: Optional[bool] = Field(None, example=True)


class RecommendationRequest(BaseModel):
    """Request model for recommendation endpoint."""
    location: str = Field(..., example="Santa Barbara, CA")
    category: str = Field(..., example="Mexican")
    filters: Optional[RecommendationFilters] = Field(None, description="Optional attribute filters")
    limit: int = Field(10, ge=1, le=50, example=10)


class BusinessSuggestion(BaseModel):
    """Individual business suggestion in recommendation response."""
    business_id: str
    business_name: str
    categories: List[str]
    stars: float
    review_count: int
    location: str
    # Attribute indicators
    has_parking: bool = False
    has_wifi: bool = False
    price_range: int = 0
    has_outdoor_seating: bool = False
    is_good_for_kids: bool = False
    has_delivery: bool = False
    has_takeout: bool = False


class RecommendationResponse(BaseModel):
    """Response model for recommendation endpoint."""
    status: str
    input: RecommendationRequest
    total_found: int
    suggestions: List[BusinessSuggestion]


# =============================================================================
# Configuration
# =============================================================================

# Azure SQL Server connection
server = get_env('AZURE_SQL_SERVER', required=True)
database = get_env('AZURE_SQL_DATABASE', required=True)
username = get_env('AZURE_SQL_USERNAME', required=True)
password = get_env('AZURE_SQL_PASSWORD', required=True)
driver = get_env('AZURE_SQL_DRIVER', '{ODBC Driver 18 for SQL Server}')
conn_str = f'DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password}'

# Event Hub configuration (optional)
event_hub_connection_str = get_env('EVENT_HUB_CONNECTION_STRING', default='')
event_hub_name = get_env('EVENT_HUB_NAME', 'eventhub1')

# Model artifacts
model_path = get_env('MODEL_PATH', 'recommendation_model.pkl')
vectorizer_path = get_env('VECTORIZER_PATH', 'tfidf_vectorizer.pkl')


# =============================================================================
# Application Lifecycle
# =============================================================================

# Global model storage
model = None
tfidf_vectorizer = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan handler for startup/shutdown events.
    Loads ML models on startup.
    """
    global model, tfidf_vectorizer
    
    logger.info("application_startup", message="Loading ML models...")
    
    try:
        if Path(model_path).exists():
            model = joblib.load(model_path)
            logger.info("model_loaded", path=model_path)
        else:
            logger.warning("model_not_found", path=model_path)
        
        if Path(vectorizer_path).exists():
            tfidf_vectorizer = joblib.load(vectorizer_path)
            logger.info("vectorizer_loaded", path=vectorizer_path)
        else:
            logger.warning("vectorizer_not_found", path=vectorizer_path)
            
    except Exception as e:
        logger.error("model_load_failed", error=str(e))
        # Don't fail startup, just log the error
    
    yield
    
    logger.info("application_shutdown", message="Cleaning up resources...")


# =============================================================================
# FastAPI Application
# =============================================================================

app = FastAPI(
    title="Yelp Recommendation API",
    description="API for Yelp business recommendations based on location and category",
    version="1.0.0",
    lifespan=lifespan,
)

# Create versioned router
router = APIRouter(prefix="/api/v1", tags=["v1"])


# =============================================================================
# Database Utilities
# =============================================================================

def get_db_connection() -> pyodbc.Connection:
    """
    Create a new database connection.
    
    Returns:
        pyodbc.Connection: Database connection object.
        
    Raises:
        pyodbc.Error: If connection fails.
    """
    return pyodbc.connect(conn_str)


async def send_to_event_hub(event_data: dict) -> None:
    """
    Send event data to Azure Event Hub asynchronously.
    
    Args:
        event_data: Dictionary containing event payload.
    """
    if not event_hub_connection_str:
        logger.debug("event_hub_skipped", reason="No connection string configured")
        return
        
    try:
        # Import here to avoid issues if azure-eventhub not installed
        from azure.eventhub import EventHubProducerClient, EventData
        
        producer = EventHubProducerClient.from_connection_string(
            conn_str=event_hub_connection_str,
            eventhub_name=event_hub_name
        )
        async with producer:
            event = EventData(json.dumps(event_data))
            await producer.send_batch([event])
            
        logger.info("event_hub_sent", event_name=event_hub_name)
        
    except ImportError:
        logger.warning("event_hub_not_available", reason="azure-eventhub not installed")
    except Exception as e:
        logger.error("event_hub_send_failed", error=str(e), event_data=event_data)


# =============================================================================
# API Endpoints
# =============================================================================

@router.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    """
    Health check endpoint.
    
    Returns:
        HealthResponse: Status of the API service.
    """
    return HealthResponse(status="ok")


@router.get("/options", response_model=OptionsResponse)
async def get_location_and_categories() -> OptionsResponse:
    """
    Get available locations and categories for recommendation filters.
    
    Returns:
        OptionsResponse: Lists of available locations and categories.
        
    Raises:
        HTTPException: 500 if database query fails.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get distinct locations
        location_query = """
        SELECT DISTINCT TOP 100 CONCAT(city, ', ', state) AS location
        FROM dim_location
        """
        cursor.execute(location_query)
        locations = [row[0] for row in cursor.fetchall()]
        
        # Get distinct categories
        category_query = """
        SELECT DISTINCT VALUE AS category
        FROM dim_category
        CROSS APPLY STRING_SPLIT(category_list, ',')
        """
        cursor.execute(category_query)
        categories = [row[0].strip() for row in cursor.fetchall()]
        
        conn.close()
        
        logger.info(
            "options_fetched",
            locations_count=len(locations),
            categories_count=len(categories)
        )
        
        return OptionsResponse(locations=locations, categories=categories)
        
    except pyodbc.Error as e:
        logger.error("database_error", error=str(e), operation="get_options")
        raise HTTPException(
            status_code=500,
            detail="Database connection failed. Please try again later."
        )
    except Exception as e:
        logger.error("unexpected_error", error=str(e), operation="get_options")
        raise HTTPException(
            status_code=500,
            detail=f"An unexpected error occurred: {str(e)}"
        )


@router.post("/recommendations", response_model=RecommendationResponse)
async def recommend_places(data: RecommendationRequest) -> RecommendationResponse:
    """
    Get business recommendations based on location, category, and optional filters.
    
    Args:
        data: Request containing location, category, and optional filters.
        
    Returns:
        RecommendationResponse: List of recommended businesses with attributes.
        
    Raises:
        HTTPException: 400 if required fields are missing, 500 if query fails.
    """
    location = data.location
    category = data.category
    filters = data.filters
    limit = data.limit
    
    if not location or not category:
        logger.warning("invalid_request", reason="Missing required fields")
        raise HTTPException(status_code=400, detail="Missing required fields: location and category")
    
    # Send to event hub for analytics (fire and forget)
    await send_to_event_hub({
        "location": location, 
        "category": category,
        "filters": filters.dict() if filters else None
    })
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Build dynamic query with filters
        query = """
        SELECT b.business_id, b.business_name, b.stars AS business_stars, 
               b.review_count, c.categories, l.city, l.state,
               ISNULL(b.has_parking, 0) as has_parking,
               ISNULL(b.has_wifi, 0) as has_wifi,
               ISNULL(b.price_range, 0) as price_range,
               ISNULL(b.has_outdoor_seating, 0) as has_outdoor_seating,
               ISNULL(b.is_good_for_kids, 0) as is_good_for_kids,
               ISNULL(b.has_delivery, 0) as has_delivery,
               ISNULL(b.has_takeout, 0) as has_takeout
        FROM dim_business b
        JOIN dim_location l ON b.business_id = l.business_id
        JOIN dim_category c ON b.business_id = c.business_id
        WHERE CONCAT(l.city, ', ', l.state) = ? 
          AND c.categories LIKE ? 
          AND b.review_count > 0
        """
        
        params = [location, f"%{category}%"]
        
        # Apply optional filters
        if filters:
            if filters.min_stars is not None:
                query += " AND b.stars >= ?"
                params.append(filters.min_stars)
            
            if filters.max_price_range is not None:
                query += " AND b.price_range <= ?"
                params.append(filters.max_price_range)
            
            if filters.has_parking is True:
                query += " AND b.has_parking = 1"
            
            if filters.has_wifi is True:
                query += " AND b.has_wifi = 1"
            
            if filters.has_outdoor_seating is True:
                query += " AND b.has_outdoor_seating = 1"
            
            if filters.is_good_for_kids is True:
                query += " AND b.is_good_for_kids = 1"
            
            if filters.has_delivery is True:
                query += " AND b.has_delivery = 1"
            
            if filters.has_takeout is True:
                query += " AND b.has_takeout = 1"
        
        # Order by stars and limit
        query += " ORDER BY b.stars DESC, b.review_count DESC"
        
        cursor.execute(query, params)
        business_data = cursor.fetchall()
        total_found = len(business_data)
        conn.close()
        
        if not business_data:
            logger.info(
                "no_recommendations_found",
                location=location,
                category=category,
                filters=filters.dict() if filters else None
            )
            return RecommendationResponse(
                status="success",
                input=data,
                total_found=0,
                suggestions=[]
            )
        
        suggestions = []
        for row in business_data[:limit]:
            (business_id, business_name, business_stars, review_count, 
             categories, city, state, has_parking, has_wifi, price_range,
             has_outdoor_seating, is_good_for_kids, has_delivery, has_takeout) = row
            
            suggestions.append(BusinessSuggestion(
                business_id=business_id,
                business_name=business_name,
                categories=categories.split(', ') if categories else [],
                stars=float(business_stars) if business_stars else 0.0,
                review_count=int(review_count) if review_count else 0,
                location=f"{city}, {state}",
                has_parking=bool(has_parking),
                has_wifi=bool(has_wifi),
                price_range=int(price_range) if price_range else 0,
                has_outdoor_seating=bool(has_outdoor_seating),
                is_good_for_kids=bool(is_good_for_kids),
                has_delivery=bool(has_delivery),
                has_takeout=bool(has_takeout)
            ))
        
        logger.info(
            "recommendations_generated",
            location=location,
            category=category,
            total_found=total_found,
            returned=len(suggestions),
            filters_applied=filters is not None
        )
        
        return RecommendationResponse(
            status="success",
            input=data,
            total_found=total_found,
            suggestions=suggestions
        )
        
    except pyodbc.Error as e:
        logger.error("database_error", error=str(e), operation="recommendations")
        raise HTTPException(
            status_code=500,
            detail="Database connection failed. Please try again later."
        )
    except Exception as e:
        logger.error("unexpected_error", error=str(e), operation="recommendations")
        raise HTTPException(
            status_code=500,
            detail=f"Error processing recommendation: {str(e)}"
        )


# Include router in app
app.include_router(router)


# =============================================================================
# Main Entry Point
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8081)
