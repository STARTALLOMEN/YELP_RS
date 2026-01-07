{{ config(materialized='view') }}

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
FROM {{ ref('fact_review_seed') }}
