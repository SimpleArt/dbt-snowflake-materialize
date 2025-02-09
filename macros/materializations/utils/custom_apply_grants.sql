{% macro custom_apply_grants(relation, grants, type=none, arguments=none) %}

    {% if not execute or grants is none %}
        {{ return(none) }}
    {% endif %}

    {% if type is none %}
        {% set type = relation.type %}
    {% endif %}

    {% set show_grants -%}
        show grants on {{ type }} {{ relation }} {{- arguments if arguments is not none }}
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

    {% for grant, grantees in adapter.standardize_grants_dict(run_query(show_grants)).items() %}

        {% for grantee in grantees %}
            {% if grantee not in delta %}
                {% do delta.update({grantee: {}}) %}
            {% endif %}

            {% do delta[grantee].update({grant: delta[grantee].get(grant, 0) - 1}) %}
        {% endfor %}

    {% endfor %}

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
            {% call statement('grant_privilege') %}
                grant {{ requires_grant | join(", ") }} on {{ type }} {{ relation }} {{- arguments if arguments is not none }} from role {{ grantee }}
            {% endcall %}
        {% endif %}

        {% if requires_revoke != [] %}
            {% call statement('revoke_privilege') %}
                revoke {{ requires_revoke | join(", ") }} on {{ type }} {{ relation }} {{- arguments if arguments is not none }} from role {{ grantee }}
            {% endcall %}
        {% endif %}

    {% endfor %}

    {{ return(none) }}

{% endmacro %}
