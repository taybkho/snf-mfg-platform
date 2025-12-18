{% macro generate_schema_name(custom_schema_name, node) -%}
  {# 
    Production-grade schema routing:
    - If a model (or folder) sets +schema: CORE / MARTS / STAGING, use it exactly.
    - Otherwise fall back to target.schema (but never append/concat).
  #}

  {% if custom_schema_name is not none and custom_schema_name | trim != "" %}
    {{ custom_schema_name | trim | upper }}
  {% else %}
    {{ target.schema | trim | upper }}
  {% endif %}

{%- endmacro %}
