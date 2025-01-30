WITH source_data AS (
    SELECT *
    FROM dim_category
)
SELECT
    category_id,
    business_id,
    category_name
FROM source_data;
