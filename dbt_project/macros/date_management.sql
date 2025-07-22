{% macro get_previous_year(i=1) %}
    {{ (modules.datetime.datetime.now().year - i) }}
{% endmacro %}


{% macro get_reference_date(reference_date=None) %}
  {% if reference_date is not none %}
    {% set ref_date = modules.datetime.datetime.strptime(reference_date, '%Y-%m-%d') %}
  {% else %}
    {% set ref_date = modules.datetime.datetime.today() %}
  {% endif %}
  {{ return(ref_date.strftime('%Y-%m-%d')) }}
{% endmacro %}


{% macro where_last_6_months(reference_date=None, date_column_year='Annee', date_column_month='MOIS') %}
  {% set ref_date = modules.datetime.datetime.strptime(reference_date, '%Y-%m-%d') %}

    {% set months_back = [] %}
    {% set year = ref_date.year %}
    {% set month = ref_date.month %}

    {% for i in range(6) %}
        {% set current_month = month - i %}
        {% if current_month <= 0 %}
            {% set m = current_month + 12 %}
            {% set y = year - 1 %}
        {% else %}
            {% set m = current_month %}
            {% set y = year %}
        {% endif %}
        {% do months_back.append((y, m)) %}
    {% endfor %}

    (
        {% for ym in months_back %}
            ({{ date_column_year }}::INT = {{ ym[0] }} AND {{ date_column_month }}::INT = {{ ym[1] }})
            {% if not loop.last %} OR {% endif %}
        {% endfor %}
    )
{% endmacro %}


{% macro where_remaining_last_year_months(reference_date=None, date_column_year='annee', date_column_month='mois') %}
  {% set ref_date = modules.datetime.datetime.strptime(reference_date, '%Y-%m-%d') %}

    {% set year = ref_date.year %}
    {% set month = ref_date.month %}

    {# Calcul du mois de début des 6 derniers mois glissants #}
    {% set sliding_start_month = month - 5 %}
    {% set sliding_start_year = year %}
    {% if sliding_start_month <= 0 %}
        {% set sliding_start_month = sliding_start_month + 12 %}
        {% set sliding_start_year = year - 1 %}
    {% endif %}

    {% set months_to_include = [] %}
    {% for m in range(1, 13) %}
        {% if m < sliding_start_month %}
            {% do months_to_include.append(m) %}
        {% endif %}
    {% endfor %}

    {# Ligne SQL #}
    (
        {{ date_column_year }}::INT = {{ sliding_start_year }}
        AND {{ date_column_month }}::INT IN (
            {{ months_to_include | join(', ') }}
        )
    )
{% endmacro %}


{% macro previous_month_condition(reference_date=None, i=1, year_col='adec', month_col='mdec') %}
  {% set ref_date = modules.datetime.datetime.strptime(reference_date, '%Y-%m-%d') %}
  
  {# Calcul du nouveau mois en reculant de i mois #}
  {% set month = ref_date.month - i %}
  {% set year = ref_date.year %}
  
  {% if month <= 0 %}
    {% set month = month + 12 %}
    {% set year = year - 1 %}
  {% endif %}
  
  {% set condition = year_col ~ '::INT = ' ~ year ~ ' AND ' ~ month_col ~ '::INT = ' ~ month %}
  
  {{ return(condition) }}
{% endmacro %}


{% macro previous_year_and_current(reference_date=None, year_col='adec', month_col='mdec') %}
    {% set ref_date = modules.datetime.datetime.strptime(reference_date, '%Y-%m-%d') %}
    {% set month = ref_date.month %}
    {% set year = ref_date.year %}
    {% set condition = '(' ~ year_col ~ '::INT = ' ~ (year - 1) | string ~
                        ' OR (' ~ year_col ~ '::INT = ' ~ year | string ~
                        ' AND ' ~ month_col ~ '::INT < ' ~ month | string ~ 
                        ' AND ' ~ month_col ~ '::INT !=7))' %}
    {{ return(condition) }}
{% endmacro %}