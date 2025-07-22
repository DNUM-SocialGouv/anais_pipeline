{% macro get_source_schema() %}
    {% if target.type == 'postgres' %}
        {{ return('public') }}
    {% else %}
        {{ return('main') }}
    {% endif %}
{% endmacro %}