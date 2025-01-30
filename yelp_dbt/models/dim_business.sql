WITH source_data AS (
    SELECT *
    FROM dim_business
)
SELECT
    business_id,
    name,
    address,
    city,
    state,
    postal_code,
    latitude,
    longitude,
    stars,
    review_count,
    is_open
FROM source_data;
