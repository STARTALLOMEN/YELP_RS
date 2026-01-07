# API Service
FastAPI service exposing recommendation and health endpoints.

## Endpoints (current)
- `GET /health` : Liveness probe.
- (Planned) `GET /recommend` : Hybrid recommendations.

## Configuration
Values loaded from environment variables. See `.env.example` for keys.

## Local Run
```bash
uvicorn api.app:app --reload --port 8000
```

## Roadmap
- Add authentication (API key / OAuth2).
- Rate limiting & request logging.
- Pydantic response models & error handling middleware.
- Observability (Prometheus metrics, structured logs, tracing).
