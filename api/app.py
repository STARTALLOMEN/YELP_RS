from fastapi import FastAPI, HTTPException
import pyodbc
from typing import List, Dict
from pydantic import BaseModel
from azure.eventhub import EventHubProducerClient, EventData
import json
import joblib
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables (.env optional)
ENV_PATH = Path(__file__).resolve().parent.parent / 'config' / '.env'
if ENV_PATH.exists():
    load_dotenv(ENV_PATH)
else:
    load_dotenv()  # fallback if user exports in shell

# Helper to fetch env with optional default or raise
def get_env(name: str, default=None, required: bool = False):
    val = os.getenv(name, default)
    if required and (val is None or val == ''):
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val

# Khởi tạo ứng dụng FastAPI
app = FastAPI()

# Kết nối đến Azure SQL Server (build connection string from env)
server = get_env('AZURE_SQL_SERVER', required=True)
database = get_env('AZURE_SQL_DATABASE', required=True)
username = get_env('AZURE_SQL_USERNAME', required=True)
password = get_env('AZURE_SQL_PASSWORD', required=True)
driver = get_env('AZURE_SQL_DRIVER', '{ODBC Driver 18 for SQL Server}')
conn_str = f'DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password}'

# Cấu hình Event Hub
event_hub_connection_str = get_env('EVENT_HUB_CONNECTION_STRING', required=True)
event_hub_name = get_env('EVENT_HUB_NAME', 'eventhub1')

# Model artifacts
model_path = get_env('MODEL_PATH', 'recommendation_model.pkl')
vectorizer_path = get_env('VECTORIZER_PATH', 'tfidf_vectorizer.pkl')

try:
    model = joblib.load(model_path)
    tfidf_vectorizer = joblib.load(vectorizer_path)
except Exception as e:
    raise RuntimeError(f"Error loading model or vectorizer: {str(e)}")

# Kết nối SQL
def get_db_connection():
    return pyodbc.connect(conn_str)

# Hàm gửi dữ liệu đến Event Hub
def send_to_event_hub(event_data: dict):
    """Gửi dữ liệu tới Azure Event Hub."""
    try:
        producer = EventHubProducerClient.from_connection_string(
            conn_str=event_hub_connection_str,
            eventhub_name=event_hub_name
        )
        with producer:
            event = EventData(json.dumps(event_data))
            producer.send_batch([event])
    except Exception as e:
        # Log silently or integrate logger
        print(f"Error sending to Event Hub: {str(e)}")

@app.get("/health")
def health_check():
    return {"status": "ok"}

# API 1: Lấy danh sách location và category
@app.get("/options", response_model=Dict[str, List[str]])
def get_location_and_categories():
    """Truy vấn danh sách location và category hợp lệ để người dùng chọn."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        location_query = """
        SELECT DISTINCT TOP 100 CONCAT(city, ', ', state) AS location
        FROM dim_location
        """
        cursor.execute(location_query)
        locations = [row[0] for row in cursor.fetchall()]
        category_query = """
        SELECT DISTINCT VALUE AS category
        FROM dim_category
        CROSS APPLY STRING_SPLIT(category_list, ',')
        """
        cursor.execute(category_query)
        categories = [row[0].strip() for row in cursor.fetchall()]
        conn.close()
        return {"locations": locations, "categories": categories}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching data: {str(e)}")

class RecommendationRequest(BaseModel):
    location: str
    category: str

# API 2: Gợi ý địa điểm
@app.post("/recommendations")
def recommend_places(data: RecommendationRequest):
    """Gợi ý địa điểm dựa trên location và category."""
    try:
        location = data.location
        category = data.category
        if not location or not category:
            raise HTTPException(status_code=400, detail="Missing required fields")
        event_data = {"location": location, "category": category}
        send_to_event_hub(event_data)
        conn = get_db_connection()
        cursor = conn.cursor()
        query = """
        SELECT b.business_id, b.business_name, b.stars AS business_stars, 
               b.review_count, c.categories, l.city, l.state
        FROM dim_business b
        JOIN dim_location l ON b.business_id = l.business_id
        JOIN dim_category c ON b.business_id = c.business_id
        WHERE CONCAT(l.city, ', ', l.state) = ? AND c.categories LIKE ? AND b.review_count > 0 AND b.stars > 4
        """
        cursor.execute(query, (location, f"%{category}%"))
        business_data = cursor.fetchall()
        if not business_data:
            return {"status": "success", "input": {"location": location, "category": category}, "suggestions": []}
        suggestions = []
        for row in business_data:
            business_id, business_name, business_stars, review_count, categories, city, state = row
            suggestions.append({
                "business_id": business_id,
                "business_name": business_name,
                "categories": categories.split(', '),
                "stars": business_stars,
                "review_count": review_count,
                "location": f"{city}, {state}"
            })
        conn.close()
        return {"status": "success", "input": {"location": location, "category": category}, "suggestions": suggestions[:10]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing recommendation: {str(e)}")
