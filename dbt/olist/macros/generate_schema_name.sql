{#
    Bawaan dbt: schema custom diberi awalan schema target, misalnya "staging_staging".
    Override ini memakai nama schema persis di dbt_project.yml (raw, staging, ...),
    sesuai 4 schmea yang dibuat warehouse/init/01_create_schemas.sql.
#}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro -%}