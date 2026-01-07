{{ config(materialized='view') }}

SELECT
    user_id,
    name,
    review_count,
    average_stars
FROM {{ ref('dim_user_seed') }}
