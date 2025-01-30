WITH source_data AS (
    SELECT *
    FROM fact_review
)
SELECT
    review_id,
    user_id,
    business_id,
    stars,
    useful,
    funny,
    cool,
    text,
    date
FROM source_data;
