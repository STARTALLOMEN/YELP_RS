-- Test: Ensure review_count is never negative
-- This is a singular test (runs against specific query)

SELECT
    business_id,
    review_count
FROM {{ ref('dim_business') }}
WHERE review_count < 0
