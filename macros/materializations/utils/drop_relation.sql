{% macro drop_relation(relation, drop_unless_type=none) %}

    {% set type = get_relation_type(relation) %}

    {% if drop_unless_type is none %}
        {% set drop_unless_type = relation.type %}
    {% endif %}

    {% if type is none %}
        {{ return(none) }}

    {% elif type == drop_unless_type %}
        {{ return(true) }}

    {% elif execute %}
        {% if kind == 'function' %}

            {% for row in run_query(show_relation(relation, 'user function')) %}

                {% set drop_query -%}
                    drop function if exists {{ relation }}{{ row['arguments'][len(relation.identifier):] }}
                {%- endset %}

                {% do run_query(drop_query) %}

            {% endfor %}

        {% elif kind == 'procedure' %}

            {% for row in run_query(show_relation(relation, 'procedure')) %}

                {% set drop_query -%}
                    drop procedure if exists {{ relation }}{{ row['arguments'][len(relation.identifier):] }}
                {%- endset %}

                {% do run_query(drop_query) %}

            {% endfor %}

        {% else %}

            {% set drop_query -%}
                drop {{ type }} if exists {{ relation }}
            {%- endset %}

            {% do run_query(drop_query) %}
        {% endif %}
    {% endif %}

    {{ return(false) }}

{% endmacro %}
