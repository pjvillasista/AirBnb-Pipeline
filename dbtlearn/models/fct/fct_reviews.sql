-- Sets the model to incrementally update an existing table with new data
-- Instructs dbt to fail the model run if it detects schema changes
{{
  config( 
    materialized = 'incremental',
    on_schema_change='fail'
    )
}}

WITH src_reviews AS (
  SELECT * FROM {{ ref('src_reviews') }}
)
-- Main SELECT statement for the model
SELECT 
  -- Generates a unique ID for each record based on specified fields
  {{ dbt_utils.generate_surrogate_key([
    'listing_id', 'review_date', 'reviewer_name', 'review_text']) }} as review_id,
  -- Selects all columns from the CTE above
  *
FROM src_reviews
-- Filters out records where review_text is null
WHERE review_text is not null
-- Conditional logic for incremental model updates
-- if it is an incremental load, we append the SQL condition
{% if is_incremental() %}
  -- Checks if start_date and end_date variables are provided
  {% if var("start_date", False) and var("end_date", False) %}
    -- Logs the incremental loading process with the specified date range
    {{ log('Loading ' ~ this ~ ' incrementally (start_date: ' ~ var("start_date") ~ ', end_date: ' ~ var("end_date") ~ ')', info=True) }}
    -- Filters records to include only those within the specified date range
    AND review_date >= '{{ var("start_date") }}'
    AND review_date < '{{ var("end_date") }}'
  {% else %}
    -- For incremental loads without specific dates, only include records newer than the latest in the table
    AND review_date > (select max(review_date) from {{ this }})
    -- Logs the incremental loading process for all missing dates
    {{ log('Loading ' ~ this ~ ' incrementally (all missing dates)', info=True)}}
  {% endif %}
{% endif %}