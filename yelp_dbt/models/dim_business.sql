{{ config(materialized='view') }}

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
FROM {{ ref('dim_business_seed') }}
