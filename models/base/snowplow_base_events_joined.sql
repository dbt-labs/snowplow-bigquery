
with events as (

    select * from {{ ref('snowplow_base_events') }}

),

web_page as (

    select * from {{ ref('snowplow_base_web_page_context') }}

)

select *,
    date_trunc(date(collector_tstamp), day) as date_day

from events
join web_page using (event_id)
