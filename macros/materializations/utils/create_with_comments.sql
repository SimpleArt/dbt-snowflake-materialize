{%- macro create_with_comments(model, sql) -%}

            {%- set descriptions = get_yaml_descriptions(model) %}

            {%- if descriptions %}
            (
                {%- for column in get_query_schema(sql) %}
                    {{ adapter.quote(column) }}
                    {%- if descriptions.get(column)
                    %} comment $${{ descriptions.get(column) }}$$
                    {%- endif %}
                    {%- if not loop.last -%} , {%- endif %}
                {%- endfor %}
            )
            {%- endif %}

            {%- if model.description %}
            comment = $${{ model.description }}$$
            {%- endif %}

{%- endmacro -%}
