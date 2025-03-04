from fastapi import FastAPI, HTTPException
import pyodbc
from typing import List, Dict

# Khởi tạo ứng dụng FastAPI
app = FastAPI()

# Kết nối đến Azure SQL Server
server = 'is402server.database.windows.net'
database = 'is402db'
username = 'trieu'
password = 'Admin003@'
driver = '{ODBC Driver 18 for SQL Server}'
conn_str = f'DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password}'

# Kết nối SQL
def get_db_connection():
    return pyodbc.connect(conn_str)

# API 1: Lấy danh sách location và category
@app.get("/options", response_model=Dict[str, List[str]])
def get_location_and_categories():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Lấy tối đa 100 locations
        location_query = """
        SELECT DISTINCT TOP 100 CONCAT(city, ', ', state) AS location
        FROM dim_location
        """
        cursor.execute(location_query)
        locations = [row[0] for row in cursor.fetchall()]

        # Lấy tối đa 100 categories
        category_query = """
        SELECT DISTINCT TOP 100 categories
        FROM dim_category
        CROSS APPLY STRING_SPLIT(category_list, ',')
        """
        cursor.execute(category_query)
        categories = [row[0].strip() for row in cursor.fetchall()]

        conn.close()
        return {"locations": locations, "categories": categories}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching data: {str(e)}")

# API 2: Thu thập thông tin người dùng
@app.post("/user-input")
def user_input(data: Dict):
    try:
        location = data.get("location")
        category = data.get("category")
        user_input_text = data.get("input")

        if not location or not category or not user_input_text:
            raise HTTPException(status_code=400, detail="Missing required fields")

        return {
            "status": "success",
            "location": location,
            "category": category,
            "user_input": user_input_text,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing input: {str(e)}")
