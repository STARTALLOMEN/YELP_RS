

WITH raw_data AS (
    SELECT *
    FROM dim_checkin
)

SELECT
    checkin_id,
    business_id,
    date,
    checkin_count
FROM raw_data
