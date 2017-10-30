
{{ config(materialized='table') }}

select distinct
    date_trunc(date(collector_tstamp), day) as date_day

from {{ var('snowplow:events_daily') }}
