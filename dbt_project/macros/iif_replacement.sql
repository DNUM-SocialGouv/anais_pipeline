{% macro iif_replacement(condition, true_val, false_val) %}
    CASE 
        WHEN {{ condition }} THEN {{ true_val }}
        ELSE {{ false_val }}
    END
{% endmacro %}