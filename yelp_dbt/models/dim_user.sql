WITH source_data AS (
    SELECT *
    FROM dim_user
)
SELECT
    user_id,
    name,
    review_count,
    yelping_since,
    useful,
    funny,
    cool,
    elite
FROM source_data;
