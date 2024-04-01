{{ config(
  materialized = 'table',
) }}

WITH fct_reviews AS (
    SELECT * FROM {{ ref('fct_reviews') }}
),
full_moon_dates AS (
    SELECT * FROM {{ ref('seed_full_moon_dates') }}
)

SELECT
  -- Selects all columns from the 'fct_reviews' CTE
  r.*,
  -- Adds a new column 'is_full_moon' to indicate if the review happened during a full moon
  CASE
    -- Checks if there is no corresponding full moon date
    WHEN fm.full_moon_date IS NULL THEN 'not full moon'
    ELSE 'full moon'
  END AS is_full_moon
FROM
  -- From the 'fct_reviews' CTE
  fct_reviews r
  -- Performs a LEFT JOIN with the 'full_moon_dates' CTE
  LEFT JOIN full_moon_dates fm
  -- Join condition that matches reviews to the day after a full moon
  ON (TO_DATE(r.review_date) = DATEADD(DAY, 1, fm.full_moon_date))