{% test is_positive(model, column_name) %}
{#
    Custom test macro: Ensures a column contains only positive values (>= 0)
    
    Usage in schema.yml:
        - name: review_count
          tests:
            - is_positive
#}

SELECT
    {{ column_name }}
FROM {{ model }}
WHERE {{ column_name }} < 0

{% endtest %}


{% test is_valid_range(model, column_name, min_value, max_value) %}
{#
    Custom test macro: Ensures a column value is within a specified range
    
    Usage in schema.yml:
        - name: stars
          tests:
            - is_valid_range:
                min_value: 1
                max_value: 5
#}

SELECT
    {{ column_name }}
FROM {{ model }}
WHERE {{ column_name }} < {{ min_value }} OR {{ column_name }} > {{ max_value }}

{% endtest %}


{% test is_not_empty_string(model, column_name) %}
{#
    Custom test macro: Ensures a string column is not empty or whitespace only
    
    Usage in schema.yml:
        - name: business_name
          tests:
            - is_not_empty_string
#}

SELECT
    {{ column_name }}
FROM {{ model }}
WHERE {{ column_name }} IS NULL 
   OR TRIM({{ column_name }}) = ''

{% endtest %}
