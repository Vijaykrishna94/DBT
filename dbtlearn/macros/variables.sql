{% macro learn_variables() %}
    {% set your_name_jinja = "Vijay"%}
    {{log("Hello "~your_name_jinja,info=True)}}
    {{log("Hello Dbt User "~var("user_name","No Username was Passed")~"!",info=True)}}
{% endmacro %}