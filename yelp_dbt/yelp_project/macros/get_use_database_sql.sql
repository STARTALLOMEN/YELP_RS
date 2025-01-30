-- macros/get_use_database_sql.sql
{% macro get_use_database_sql(database) %}
    USE [{{ database }}];
{% endmacro %}
