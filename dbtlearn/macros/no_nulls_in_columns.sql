-- This macro is designed to check for null values across all columns of a given model.
-- It dynamically generates a SQL query that selects all rows from the specified model
-- where any column contains a null value. This can be useful for data quality checks,
-- ensuring that your models don't contain unexpected nulls.

{% macro no_nulls_in_columns(model) %}
    -- The SELECT statement begins the query to retrieve all rows from the input model.
    SELECT * FROM {{ model }} WHERE
    -- A loop iterates over each column in the specified model, checking for null values.
    {% for col in adapter.get_columns_in_relation(model) -%}
        -- For each column, this condition adds a check to see if the column is NULL.
        {{ col.column }} IS NULL OR
    {% endfor %}
    FALSE
{% endmacro %}
