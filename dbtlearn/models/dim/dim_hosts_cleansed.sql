{{
  config(
    materialized = 'view'
    )
}} 
WITH src_hosts AS(
    SELECT * FROM {{ ref('src_hosts') }}
)
SELECT
    host_id,
    -- If host_name is not null, keep the original value
    -- If host_name is null, replace it with the value ‘Anonymous’
    NVL(host_name, 'Anonymous') AS host_name,
    is_superhost,
    created_at,
    updated_at
FROM src_hosts