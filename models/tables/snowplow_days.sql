
{{ config(materialized='table') }}

select distinct
    date_trunc(date(collector_tstamp), day) as date_day

from {{ ref('snowplow_base_events_joined') }}
