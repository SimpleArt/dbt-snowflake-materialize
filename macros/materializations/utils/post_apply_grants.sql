{% macro post_apply_grants(relation, grants, should_revoke=true, type=none, arguments=none) %}

    {% if not execute or grants is none %}
        {{ return(none) }}
    {% endif %}

    {% if type is none %}
        {% set type = relation.type %}
    {% endif %}

    {% set show_grants -%}
        show grants on {{ type }} {{ relation }}
        {%- if arguments is not none -%}
        ({{ arguments }})
        {%- endif %}
    {%- endset %}
    {% set delta = {} %}

    {% for grant, grantees in grants.items() %}
        {% set grant = quote_unquoted(grant) %}

        {% for grantee in grantees %}
            {% set grantee = quote_unquoted(grantee) %}

            {% if grantee not in delta %}
                {% do delta.update({grantee: {}}) %}
            {% endif %}

            {% do delta[grantee].update({grant: 1}) %}
        {% endfor %}

    {% endfor %}


    {% if should_revoke %}
        {% for grant, grantees in adapter.standardize_grants_dict(run_query(show_grants)).items() %}

            {% for grantee in grantees %}
                {% if grantee not in delta %}
                    {% do delta.update({grantee: {}}) %}
                {% endif %}

                {% do delta[grantee].update({grant: delta[grantee].get(grant, 0) - 1}) %}
            {% endfor %}

        {% endfor %}
    {% endif %}

    {% for grantee, grants in delta.items() %}
        {% set requires_grant = [] %}
        {% set requires_revoke = [] %}

        {% for grant, flag in grants.items() %}

            {% if flag > 0 %}
                {% do requires_grant.append(grant) %}
            {% elif flag < 0 %}
                {% do requires_revoke.append(grant) %}
            {% endif %}

        {% endfor %}

        {% if requires_grant != [] %}
            {% set grant_query -%}
                grant {{ requires_grant | join(", ") }} on {{ type }} {{ relation }} from role {{ grantee }}
            {%- endset %}

            {% do run_query(grant_query) %}
        {% endif %}

        {% if requires_revoke != [] %}
            {% set revoke_query -%}
                revoke {{ requires_revoke | join(", ") }} on {{ type }} {{ relation }} from role {{ grantee }}
            {%- endset %}

            {% do run_query(revoke_query) %}
        {% endif %}

    {% endfor %}

    {{ return(none) }}

{% endmacro %}
