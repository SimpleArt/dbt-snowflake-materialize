{{ config(
    materialized='function',
    parameters='client_id varchar, client_secret varchar',
    returns='varchar',
    language={
        "python": [
            "runtime_version = 3.11",
            "packages = ('requests')",
            "handler = 'get_access_token'",
            "external_access_integrations = (classy_external_access_integration)"
        ]
    }
) }}

import requests

def get_access_token(client_id, client_secret):
    data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret
    }
    return requests.post('https://api.classy.org/oauth2/auth', data=data).json()['access_token']
