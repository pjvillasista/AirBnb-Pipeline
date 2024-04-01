-- Begin definition of a snapshot called scd_raw_listings.
-- This snapshot tracks changes in the listings data over time.
{% snapshot scd_raw_listings %}

-- Configuration for the snapshot:
-- - The snapshot is stored in the 'DEV' schema.
-- - Uses the 'id' column as a unique identifier for each record.
-- - Employs a 'timestamp' strategy to detect changes, based on the 'updated_at' column.
-- - Marks records as invalid if they have been hard deleted (i.e., they no longer appear in the source data).
-- - if True = if any record is deleted in the original input table, the snapshot will reflect that; it's the end of the lifecycle of the record
{{
   config(
       target_schema='DEV',
       unique_key='id',
       strategy='timestamp',
       updated_at='updated_at',
       invalidate_hard_deletes=True
   )
}}

-- Selects all records from the 'listings' table in the 'airbnb' source.
-- This source data is what will be snapshotted, allowing dbt to track how each record changes over time.
select * FROM {{ source('airbnb', 'listings') }}

-- End of the snapshot definition.
{% endsnapshot %}
