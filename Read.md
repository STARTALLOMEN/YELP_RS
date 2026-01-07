# Kế hoạch Cải Tiến & Chuẩn Hóa Dự Án YELP_RS

File này tổng hợp TẤT CẢ các hạng mục cần điều chỉnh. Khi hoàn thành một mục, sẽ đánh dấu vào ô tương ứng.

---
## 1. Bảo mật & Cấu hình
- [x] Tách toàn bộ secrets sang biến môi trường / secret manager
- [x] Thay thế mọi đường dẫn tuyệt đối bằng đường dẫn tương đối hoặc qua config
- [x] Thêm cơ chế phân tách môi trường: `dev / staging / prod` (khởi tạo qua biến môi trường dự kiến)
- [x] Chuẩn hoá file `.env.example` (đủ biến, mô tả rõ)
- [x] Thêm script kiểm tra thiếu biến môi trường khi khởi chạy (`scripts/check_env.py`)
- [ ] Tích hợp Secret Manager (Azure Key Vault) (tùy chọn giai đoạn sau)
- [ ] Mã hoá lưu trữ model artifacts (nếu chứa thông tin nhạy cảm)

### Acceptance Criteria
Có thể deploy mà không cần hard-code secrets; chạy CI không lộ khoá; rà soát repo không còn chuỗi nhạy cảm.

---
## 2. Repository Restructuring & Modularization
- [x] Standardize top-level folders (api/, processing/, kafka/, airflow/, dbt/, models/, docs/, tests/) (IN PROGRESS – partial)
- [x] Identify & remove duplicate dbt project structure (root vs nested) (ANALYZED – removal pending)
- [x] Create processing/silver module & migrate BusinessSilver logic to python module (`processing/silver/business_transform.py`)
- [x] Add per-folder README templates (api, processing, Kafka). (Initial set done)
- [x] Add high-level architecture diagram (`docs/architecture.mmd` in Mermaid)
- [x] Migrate remaining Silver notebooks (Review, User, Checkin, Tip) into modules
- [x] Add tests for migrated modules under `tests/processing` (create scaffold)

### Acceptance Criteria
Repo rõ ràng, người mới có thể hiểu data flow trong 5 phút.

---
## 3. Spark / ETL (Bronze → Silver → Gold)
- [x] Tạo chuẩn pipeline module hóa: `ingest -> clean -> enrich -> publish`
- [x] Chuẩn hoá đặt tên bảng Delta: `bronze.*`, `silver.*`, `gold.*`
- [x] Thêm metadata cột: `_ingest_ts`, `_batch_id`, `_source`
- [x] Thêm incremental load (không overwrite toàn bộ) (merge into Delta for all Silver tables)
- [x] Bật/kiểm tra cấu hình: AQE, optimizeWrite, autoCompact (set in `processing/common/spark_session.py`)
- [x] Thêm `OPTIMIZE` / `VACUUM` schedule (Airflow)
- [x] Partition strategy đánh giá lại (tránh skew state)
- [ ] Tách rộng bảng attributes thành struct hoặc bảng phụ (giảm chiều rộng)

### Acceptance Criteria
Chạy lại full pipeline không lỗi, incremental xử lý đúng, dung lượng lưu trữ tối ưu hơn.

---
## 4. Streaming / Kafka
- [x] Thêm retry + exponential backoff chuẩn
- [x] Thêm logging chuẩn JSON cho producer
- [x] Schema contract (Avro/JSON Schema) + registry (nếu mở rộng)
- [x] Ghi offset / checkpoint chuẩn (Structured Streaming consumer)
- [ ] Tối ưu rate-limit theo quota Yelp
- [ ] Thêm cảnh báo khi rỗng dữ liệu > N chu kỳ

### Acceptance Criteria
Producer không rơi silent fail, có giám sát rate / error.

---
## 5. Airflow Orchestration
- [x] Viết DAG hoàn chỉnh: ingestion → bronze → silver → gold → feature → train → evaluate → register → serve
- [x] Thêm dependency rõ ràng (>>)
- [x] Thêm SLA + email/Slack alert
- [x] Dùng `Variables` / `Connections` thay hard-code
- [x] Logging task rõ ràng + XCom cho metrics
- [x] Thêm sensor kiểm tra availability dữ liệu trước khi run

### Acceptance Criteria
DAG chạy end-to-end có thể tái chạy idempotent.

---
## 6. Machine Learning Lifecycle
- [ ] Tách training ra script (không nằm trong notebook)
- [ ] Dùng MLflow tracking (params, metrics, artifacts)
- [ ] Đăng ký model version (staging → production)
- [ ] Thêm evaluate pipeline (RMSE, coverage, diversity)
- [ ] Lưu latent factors (user/item) dạng Delta để phục vụ nhanh
- [ ] Xây module re-ranking (hybrid + business rules + diversity)
- [ ] Chuẩn bị feature store (Feast hoặc simple Delta layer)

### Acceptance Criteria
Có thể xem lịch sử training, rollback model, phục vụ inference ổn định.

---
## 7. API Layer (FastAPI)
- [ ] Thêm middleware: request ID, timing, structured logging
- [ ] Thêm rate limiting (Redis / simple token bucket)
- [ ] Thêm authentication (API Key / OAuth placeholder)
- [ ] Thêm cache (Redis) cho các endpoint tĩnh (options)
- [ ] Chuẩn hóa schema response (pydantic models rõ ràng)
- [ ] Tách services / repositories layer
- [ ] Thêm /health /metrics (Prometheus)
- [ ] Thêm unit test API (pytest + testclient)

### Acceptance Criteria
API đạt latency ổn định < 200ms cho các request cơ bản, có logs & metrics.

---
## 8. dbt Modeling
- [ ] Chuẩn hoá sources.yml (khai báo lineage)
- [ ] Viết tests: not_null, unique, accepted_values
- [ ] Thêm macros chung (date spine, surrogate keys)
- [ ] Snapshot SCD (business status / categories biến động)
- [ ] Document models (schema.yml mô tả cột)
- [ ] Exposures cho API/report

### Acceptance Criteria
`dbt run && dbt test` pass với >95% coverage tests.

---
## 9. Data Quality & Validation
- [ ] Tích hợp Great Expectations hoặc Deequ
- [ ] Expectation suites: ranges, uniqueness, referential integrity
- [ ] Chặn pipeline nếu lỗi critical
- [ ] Lưu kết quả validation (Delta + dashboard)
- [ ] Thêm anomaly detection (z-score đơn giản ban đầu)

### Acceptance Criteria
Sai lệch chất lượng bị phát hiện trước khi tới Gold/ML.

---
## 10. Deployment & CI/CD
- [ ] Thêm `requirements.txt` + lock file (pip-tools/poetry)
- [ ] Docker hóa: api, spark driver job image, airflow, kafka (nếu cần)
- [ ] GitHub Actions: lint → test → build → deploy
- [ ] Tag version theo semver
- [ ] Tách config theo môi trường build
- [ ] Smoke test sau deploy

### Acceptance Criteria
Push branch tạo tự động pipeline kiểm thử; merge main auto build & deploy dev.

---
## 11. Observability & Monitoring
- [ ] Logging chuẩn JSON (structlog / loguru)
- [ ] Metrics: ingestion_rate, model_latency, recommendation_hit_ratio
- [ ] Tracing: OpenTelemetry (API + critical pipelines)
- [ ] Dashboards: Grafana / Azure Monitor
- [ ] Alert rules: error_rate > threshold, data lag

### Acceptance Criteria
Có thể truy vết 1 request end-to-end.

---
## 12. Performance & Scalability
- [ ] Broadcast join dimension nhỏ
- [ ] Repartition hợp lý theo business_id / user_id trong ML prep
- [ ] Adaptive Query Execution bật & log hiệu quả
- [ ] Delta OPTIMIZE + ZORDER (business_id, user_id)
- [ ] Precompute top-N per segment (city/category)
- [ ] Batch inference cache

### Acceptance Criteria
Thời gian xử lý ETL & latency inference giảm rõ rệt (>20%).

---
## 13. Feature Engineering Chuẩn Hoá
- [ ] Quy ước: `feat_` prefix cho cột dùng cho ML
- [ ] Lưu catalog features (Delta + JSON spec)
- [ ] Kiểm soát drift: so sánh distribution feat_* theo batch
- [ ] Chuẩn hoá pipeline generate features (deterministic)

### Acceptance Criteria
Có thể tái tạo cùng feature vector ở training & serving.

---
## 14. Naming Conventions & Consistency
- [ ] Chuẩn hoá snake_case toàn bộ cột
- [ ] Thống nhất timezone (UTC) cho timestamp
- [ ] Tiền tố metadata: `_` (vd: `_ingest_ts`)
- [ ] Hash keys (md5) cho surrogate nếu cần

### Acceptance Criteria
Không còn cột mixed-case hoặc ambiguous naming.

---
## 15. Notebooks Governance
- [ ] Di chuyển logic sản xuất ra scripts
- [ ] Notebooks chỉ exploratory (ghi chú rõ)
- [ ] Thêm pre-commit clear output
- [ ] Dùng Papermill khi cần parameterized run

### Acceptance Criteria
Commit không còn output lớn / artifacts.

---
## 16. Testing Strategy
- [ ] Unit tests: utils, transformations (pytest + Spark local)
- [ ] Integration tests: pipeline mini sample dataset
- [ ] API tests: contract & error cases
- [ ] ML tests: overfit guard, metric threshold
- [ ] Load test API (locust / k6) cơ bản

### Acceptance Criteria
Coverage > 70% modules cốt lõi; ML không tụt metric < baseline.

---
## 17. Data Governance & Lineage
- [ ] Data catalog (OpenMetadata / Amundsen) (phase sau)
- [ ] Lineage hiển thị: source → bronze → silver → gold → ml
- [ ] Gắn classification tags (PII: none xác nhận)
- [ ] Chính sách retention (raw vs curated)

### Acceptance Criteria
Có thể truy vết nguồn gốc bất kỳ trường Gold.

---
## 18. Model Serving & Inference
- [ ] Chuẩn hoá endpoint `/recommendations` dùng hybrid engine module
- [ ] Thêm explainability basic (trả về lý do xếp hạng: CF / content / popularity)
- [ ] Batch precompute nightly top recommendations
- [ ] A/B testing framework (flag A vs B ranker)

### Acceptance Criteria
Dịch vụ gợi ý ổn định, dễ mở rộng chiến lược xếp hạng.

---
## 19. Risk & Fallback Strategy
- [ ] Fallback khi ALS model lỗi → popularity baseline
- [ ] Circuit breaker cho external services
- [ ] Thử nghiệm chaos nhỏ (tắt Kafka / DB tạm) kiểm tra resilience

### Acceptance Criteria
System không “chết hẳn” khi 1 thành phần lỗi.

---
## 20. Tài Liệu & Onboarding
- [ ] Cập nhật README chính tổng quan
- [ ] Thêm hướng dẫn chạy local & docker
- [ ] Sơ đồ data flow + ML flow
- [ ] FAQ (lỗi thường gặp / cách fix)

### Acceptance Criteria
Thành viên mới thiết lập môi trường < 30 phút.

---
## PHÂN KỲ THỰC HIỆN (GỢI Ý)
| Giai đoạn | Mục tiêu chính |
|-----------|----------------|
| Phase 1 | Bảo mật, cấu hình, refactor ETL cơ bản, repo structure |
| Phase 2 | Airflow DAG chuẩn, MLflow, dbt tests, API hardening |
| Phase 3 | Feature store, observability nâng cao, re-ranking, A/B testing |
| Phase 4 | Governance, lineage automation, scale & optimization |

---
## Ghi chú
- Chỉ đánh dấu "x" khi có PR/commit liên quan.
- Có thể bổ sung mục mới nếu phát sinh yêu cầu.

---
## Nhật ký Đánh dấu (log)
| Ngày | Mục đã hoàn thành | Commit/PR |
|------|--------------------|-----------|
| 2025-01-08 | Task 3.3-3.4: Pipeline modularization & naming standardization | Enhanced framework |
| 2025-01-08 | Task 3.5-3.6: OPTIMIZE/VACUUM schedule & partition strategy | Delta optimization |
| 2025-01-08 | Task 4: Streaming/Kafka enhancements | Enhanced producer/consumer |
| 2025-01-08 | Task 5: Complete Airflow orchestration DAG | End-to-end pipeline |

---

> Vui lòng yêu cầu mục nào bạn muốn bắt đầu, tôi sẽ triển khai tuần tự.
