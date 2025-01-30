WITH source_data AS (
    SELECT *
    FROM dim_location
)
SELECT
    location_id,
    city,
    state,
    country,
    postal_code
FROM source_data;
