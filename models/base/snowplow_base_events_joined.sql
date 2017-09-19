
{{ config(materialized='view') }}



with events as (

    select * from {{ ref('snowplow_base_events') }}

),

web_page as (

    select * from {{ ref('snowplow_base_web_page_context') }}

)

-- colocate all events for a given session in the same date partition
select *,
    date(min(collector_tstamp) over (partition by domain_sessionid)) as date_day

from events
join web_page using (event_id)
