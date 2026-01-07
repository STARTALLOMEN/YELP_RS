-- Test: Ensure star ratings are within valid range (1.0 to 5.0)

SELECT
    review_id,
    stars
FROM {{ ref('fact_review') }}
WHERE stars < 1 OR stars > 5
