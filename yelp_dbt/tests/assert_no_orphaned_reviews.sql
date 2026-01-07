-- Test: Ensure no orphaned reviews (reviews without valid business)

SELECT
    r.review_id,
    r.business_id
FROM {{ ref('fact_review') }} r
LEFT JOIN {{ ref('dim_business') }} b ON r.business_id = b.business_id
WHERE b.business_id IS NULL
