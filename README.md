# YELP_RS
Hệ thống Recommendation & Data Platform cho Yelp Academic Dataset.

## 1. Thành phần chính
- Ingestion / Streaming: Kafka producer (Yelp API) (`Kafka/producer.py`)
- Bronze / Silver ETL: Spark + Delta (`processing/`)
- Model Training & Serving: FastAPI (`api/`), model artifacts (`api/models/`)
- Orchestration: Airflow (folder `Airflow/`)
- Analytics / DW: dbt (`yelp_dbt/`)
- Tests: `tests/`
- Tài liệu / kiến trúc: `docs/`
- Script tiện ích: `scripts/`

## 2. Yêu cầu hệ thống
| Thành phần | Yêu cầu |
|------------|---------|
| Python     | >= 3.11 |
| Java (JDK) | 8/11 (Spark) |
| Kafka      | Local cluster / Docker |
| ODBC Driver| SQL Server (nếu dùng Azure SQL) |

## 3. Cài đặt (Windows PowerShell ví dụ)
```powershell
python -m venv venv
./venv/Scripts/Activate.ps1
pip install --upgrade pip
pip install -r requirements.txt
```

## 4. Cấu hình Môi Trường
1. Copy `.env.example` thành `.env`
2. Điền các biến bắt buộc: Azure SQL (nếu dùng), YELP_API_KEY, Kafka, đường dẫn dữ liệu, model paths.
3. Kiểm tra:
```powershell
python scripts/check_env.py --strict
```
4. Phân tách môi trường:
```powershell
$env:APP_ENV = "dev"  # hoặc staging, prod
$env:BATCH_ID = "run_20250101_01"
```

## 5. Dữ liệu Yelp
Tải từ: https://www.yelp.com/dataset
Giải nén vào `./data/bronze/` (đặt các file: yelp_academic_dataset_business.json, review.json, user.json, checkin.json, tip.json) và cập nhật biến môi trường tương ứng (BRONZE_*_PATH).

## 6. Chạy Kafka Producer (Ingestion)
```powershell
$env:YELP_API_KEY = "<key>"
python Kafka/producer.py
```
(Chỉnh `INGEST_LOCATIONS` trong `.env` nếu cần.)

## 7. Chạy ETL Silver ví dụ
```powershell
python -m processing.silver.business_transform
python -m processing.silver.review_transform
```
Tùy chọn batch id:
```powershell
$env:BATCH_ID = "batch_20250101_02"; python -m processing.silver.user_transform
```

## 8. Chạy API
```powershell
uvicorn api.app:app --reload --port 8000
```
Health check: http://localhost:8000/health
Recommendations: POST /recommendations

## 9. Chạy Tests
```powershell
pytest -q
```

## 10. Airflow (đơn giản)
Thư mục `Airflow/` chứa `docker-compose.yml`. Khởi động:
```powershell
docker compose -f Airflow/docker-compose.yml up -d
```
(Thêm DAGs thực thi pipeline sau.)

## 11. dbt
Đi vào `yelp_dbt/` (hoặc hợp nhất project—hiện còn bản lặp). Cấu hình `profiles.yml` tùy kho dữ liệu mục tiêu rồi:
```powershell
dbt run
```

## 12. Kiến trúc
Sơ đồ: `docs/architecture.mmd` (Mermaid). Có thể xem trên VS Code extension hoặc chuyển PlantUML nếu cần.

## 13. Bảo mật & Secrets
- Không commit `.env`
- Có thể tích hợp Azure Key Vault (dự kiến) dùng sdk `azure-identity` + `azure-keyvault-secrets`.
- Script kiểm tra thiếu biến: `scripts/check_env.py`

## 14. Incremental & Metadata
Mỗi bảng Silver thêm `_ingest_ts`, `_batch_id`, `_source` và merge incremental (Delta MERGE) theo khóa tự nhiên.

## 15. Tối ưu Spark
Cấu hình AQE, optimizeWrite, autoCompact thiết lập trong `processing/common/spark_session.py`.

## 16. Mở rộng kế tiếp
- Thiết lập Gold layer & Feature Store
- MLflow tracking & model registry
- Great Expectations / Deequ cho chất lượng dữ liệu
- Lịch OPTIMIZE / VACUUM (Airflow DAG)

## 17. Mã hoá model (tuỳ chọn)
Có thể dùng thư viện `cryptography` để mã hoá file mô hình trước khi commit/lưu (khuyến nghị khi chứa embedding nhạy cảm).

## 18. Vấn đề thường gặp
| Lỗi | Nguyên nhân | Khắc phục |
|-----|-------------|-----------|
| pyodbc.Error | Thiếu ODBC Driver | Cài Driver 18 SQL Server |
| Java gateway error | Chưa cài JDK | Cài JDK và set JAVA_HOME |
| ModuleNotFoundError delta | Chưa cài delta-spark | pip install delta-spark |
| Kafka connection refused | Kafka chưa chạy | Khởi động cluster hoặc chỉnh KAFKA_BOOTSTRAP_SERVERS |

## 19. License / Dataset
Yelp Academic Dataset có điều khoản riêng – đảm bảo tuân thủ khi sử dụng.

## 20. Liên hệ / Đóng góp
Tạo issue / PR với mô tả rõ ràng; tuân thủ checklist cải tiến trong `Read.md`.
