
-- {{ ref('snowplow_days') }}
-- {{ ref('snowplow_page_views') }}

{{
    config(
        materialized='date_partitioned_table',
        date_source='snowplow_days',
        date_field='date_day',
        date_partitioned_base='snowplow_page_views',
        filter_output=False
    )
}}

with page_views as (

    select * from __dbt_source_data

),


sessions as (
  select
    domain_sessionid,
    min(collector_tstamp) as session_start,
    max(collector_tstamp) as session_end,
    sum(pings.agg.time_engaged_in_s) as time_engaged_in_s,

    array_agg(struct(
        app_id,
        br_type,
        domain_userid,
        domain_sessionidx,
        user_id,
        user_ipaddress,
        dvce_ismobile,
        dvce_type,
        geo,
        utm,
        referrer
      )
      order by collector_tstamp asc
    )[safe_offset(0)] as details,

    array_agg(struct(
        page_view_id,
        page,
        referrer,
        pings.agg as pings
      )
      order by collector_tstamp asc
    ) as pageviews,

    array_concat_agg(forms) as forms

  from page_views
  group by 1

),

sessions_xf as (

  select
    domain_sessionid,

    details.domain_userid,
    details.domain_sessionidx,
    details.user_id,
    details.user_ipaddress,
    details.app_id,
    details.br_type,
    details.dvce_ismobile,
    details.dvce_type,
    details.geo,
    details.utm,

    session_start,
    session_end,

    struct(
      pageviews[safe_offset(0)].page.url_path as first_page_path,
      pageviews[safe_offset(array_length(pageviews) - 1)].page.url_path as exit_page_path,
      pageviews[safe_offset(0)].page.url as first_page_url,
      pageviews[safe_offset(array_length(pageviews) - 1)].page.url as exit_page_url
    ) as overview,

    array_length(pageviews) as count_pageviews,
    time_engaged_in_s,
    pageviews,

    coalesce(array_length(forms), 0) as count_form_submissions,
    forms as form_submissions

  from sessions

)

select *
from sessions_xf
