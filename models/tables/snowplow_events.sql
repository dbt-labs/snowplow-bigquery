
-- {{ ref('snowplow_days') }}

{{
    config(
        materialized='date_partitioned_table',
        date_source='snowplow_days',
        date_field='date_day',
        filter_output=True
    )
}}

select * from {{ ref('snowplow_base_events_joined') }}
