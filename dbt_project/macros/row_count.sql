{% macro log_row_count(table_name) %}
    {% if execute %}
        {% set table_str = table_name | string %}
        {% set query = "SELECT COUNT(*) AS row_count FROM " ~ table_str %}

        {% set result = run_query(query) %}
        {% if result %}
            {% set row_count = result.columns[0].values()[0] %}
            {{ log("📊 Table '" ~ table_str ~ "' contient " ~ row_count ~ " lignes.", info=True) }}
        {% endif %}
    {% endif %}
{% endmacro %}
