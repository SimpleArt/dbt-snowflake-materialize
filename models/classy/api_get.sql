{{ config(
    materialized='function',
    parameters='params object',
    returns='variant',
    language={
        "python": [
            "runtime_version = 3.11",
            "packages = ('requests')",
            "handler = 'api_get'",
            "external_access_integrations = (classy_external_access_integration)"
        ]
    }
) }}

{#- Use the following CLI arguments: #}
    {#- dbt run --vars '{"classy_client_id": "value", "classy_client_secret": "value"}' #}

{%- set client_id = var('classy_client_id') %}
{%- set client_secret = var('classy_client_secret') %}

{%- set get_access_token -%}
select {{ ref('get_access_token') }}($${{ client_id }}$$, $${{ client_secret }}$$) as access_token
{%- endset -%}

{%- if execute %}
    {%- set access_token = run_query(get_access_token)[0]['ACCESS_TOKEN'] %}
{%- endif %}

import requests

def api_get(params):
    client_id = '{{ client_id }}'
    client_secret = '{{ client_secret }}'
    headers = {
        'Authorization': 'Bearer {{ access_token }}',
        'Content-Type': 'application/json',
    }
    url = params.pop("url").format(client_id=client_id, client_secret=client_secret)
    return requests.get(url, headers=headers, **params).json()
