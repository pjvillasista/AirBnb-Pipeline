{{
  config(
    materialized = 'view'
    )
}} 
WITH src_listings AS (
    SELECT * FROM {{ ref('src_listings') }}
)
SELECT 
  listing_id,
  listing_name,
  room_type,
  -- Minimum nights 0 does not make sense in the data
  CASE
    WHEN minimum_nights = 0 THEN 1
    ELSE minimum_nights
  END AS minimum_nights,
  host_id,
  -- Parse the string price to a number
  REPLACE(
    price_str,
    '$'
  ) :: NUMBER(
    10,
    2
  ) AS price,
  created_at,
  updated_at
FROM
  src_listings