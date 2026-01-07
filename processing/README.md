# Processing Layer

This layer holds transformation logic that converts raw Bronze data into curated Silver and aggregated Gold datasets.

## Structure
- `silver/` : Record-level cleansing, parsing, standardization.
- `gold/` (planned): Aggregations, dimensional/fact modeling, feature derivations.

## Guidelines
1. Keep notebooks exploratory only; production logic must reside in python modules.
2. All functions should be pure / side-effect free except explicit IO helpers.
3. Use environment variables for all paths (see root `.env.example`).
4. Prefer DataFrame API over UDFs; vectorize transformations.
5. Add unit tests under `tests/processing` for critical transformations.
6. Enforce data contracts (schema + semantic rules) before writing Silver/Gold.

## Running Example
```bash
python -m processing.silver.business_transform
```

## Roadmap
- Migrate remaining Silver notebooks (review, user, checkin, tip).
- Introduce validation layer (Great Expectations / Deequ).
- Add Gold aggregation modules.
- Implement feature store extraction scripts.
