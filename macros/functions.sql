{% macro margin_percent(revenue, purchase_cost, decimal_places=2) %}
    ROUND(SAFE_DIVIDE({{ revenue }} - {{ purchase_cost }}, {{ revenue }} ) * 100, {{ decimal_places }})
{% endmacro %}