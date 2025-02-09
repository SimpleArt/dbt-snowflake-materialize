{# No incremental logic because I'm lazy. #}
{# This probably should be changed into an incremental model eventually. #}
{{ config(materialized='persistent_table', tmp_relation_type='table') }}

with
    params as (
        select
            'https://api.classy.org/2.0/apps/{client_id}/organizations' as url,
            null as fields,
            [] as filters
    ),

    pagination(url, fields, filters, response, page) as (
        select
            *,
            {{ ref('api_get') }}({
                'url': url,
                'params': {
                    'fields': fields,
                    'filter': nullif(array_to_string(filters, ','), ''),
                    'sort': 'id',
                    'per_page': 100
                }
            }) as response,
            1 as page
        from
            params

        union all

        select
            url,
            fields,
            filters,
            {{ ref('api_get') }}({
                'url': url,
                'params': {
                    'fields': fields,
                    'filter': array_to_string(array_append(filters, 'id>' || response:data[99]:id), ','),
                    'sort': 'id',
                    'per_page': 100
                }
            }),
            page + 1
        from
            pagination
        where
            response:total < 100
    ),

    flattened as (
        select
            "VALUE":id::int as organization_id,
            replace("VALUE":created_at::varchar, 'T', ' ')::timestamp_ntz as created_at,
            replace("VALUE":updated_at::varchar, 'T', ' ')::timestamp_ntz as updated_at,
            "VALUE":address::varchar(127) as address,
            "VALUE":are_activity_feeds_disabled::boolean as are_activity_feeds_disabled,
            "VALUE":allow_download_annual_receipts::boolean as allow_download_annual_receipts,
            "VALUE":city::varchar(50) as city,
            "VALUE":country::varchar as country,
            "VALUE":currency_code::varchar as currency_code,
            "VALUE":description::varchar as description,
            "VALUE":facebook::varchar(150) as facebook,
            "VALUE":fixed_fot_percent::float as fixed_fee_on_top_percent,
            "VALUE":flex_rate_percent::float as flex_rate_percent,
            "VALUE":has_double_the_donation_enployer_match::boolean as has_double_the_donation_employer_match,
            "VALUE":has_employer_match::boolean as has_employer_match,
            "VALUE":is_gift_aid_available::boolean as is_gift_aid_available,
            "VALUE":logo_id::varchar as logo_id,
            "VALUE":mission::varchar as mission,
            "VALUE":name::varchar(127) as name,
            "VALUE":opt_in_wording::varchar(255) as opt_in_wording,
            "VALUE":plan_type::varchar as plan_type,
            "VALUE":postal_code::varchar as postal_code,
            "VALUE":restricted_country_information::varchar as restricted_country_information,
            "VALUE":resend_receipt_flag::boolean as resend_receipt_flag,
            "VALUE":signature_name::varchar(50) as signature_name,
            "VALUE":signature_title::varchar(50) as signature_title,
            "VALUE":signature_url::varchar as signature_url,
            "VALUE":state::varchar as state,
            "VALUE":status::varchar as status,
            "VALUE":thumbnail::varchar as thumbnail,
            "VALUE":timezone_identifier::varchar as timezone_identifier,
            "VALUE":twitter::varchar(50) as twitter,
            "VALUE":privacy_policy_url::varchar(150) as privacy_policy_url,
            "VALUE":type::varchar as "TYPE",
            "VALUE":url::varchar as url,
            "VALUE":welcome_message::varchar(100) as welcome_message
        from
            pagination,
            lateral flatten(input => response:data)
    )

select * from flattened
