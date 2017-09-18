
-- {{ ref('snowplow_days') }}
-- {{ ref('snowplow_events') }}

{{
    config(
        materialized='date_partitioned_table',
        date_source='snowplow_days',
        date_field='date_day',
        date_partitioned_base='snowplow_events',
        filter_output=False
    )
}}

with events as (

    select * from __dbt_source_data

),

form_submissions as (

  select * from {{ ref('snowplow_form_submissions') }}

),

form_submissions_agg as (

  select
    page_view_id,
    array_agg(struct(page_view_id, form_id, classes, elements)) as forms

  from form_submissions
  group by 1

),

page_views as (

  select
    page_view_id,
    app_id,
    br_type,
    collector_tstamp,
    domain_sessionid,
    domain_sessionidx,
    domain_userid,
    dvce_ismobile,
    dvce_type,
    user_id,
    user_ipaddress,

    struct(
      geo_city as city,
      geo_country as country,
      geo_latitude as latitude,
      geo_longitude as longitude,
      geo_region as region,
      geo_region_name as region_name,
      geo_timezone as timezone,
      geo_zipcode as zipcode
    ) as geo,

    struct(
      mkt_campaign as campaign,
      mkt_content as content,
      mkt_medium as medium,
      mkt_source as source,
      mkt_term as term
    ) as utm,

    struct(
      os_family as family,
      os_manufacturer as manufacturer,
      os_name as name,
      os_timezone as timezone
    ) as os,

    struct(
      page_referrer as referrer,
      page_title as title,
      page_url as url,
      page_urlfragment as url_fragment,
      page_urlhost as url_host,
      page_urlpath as url_path,
      page_urlquery as url_query,
      page_urlscheme as url_scheme
    ) as page,

    struct(
      refr_medium as medium,
      refr_source as source,
      refr_term as term,
      refr_urlfragment as url_fragment,
      refr_urlhost as url_host,
      refr_urlpath as url_path,
      refr_urlquery as url_query,
      refr_urlscheme as url_scheme
    ) as referrer,

    row_number() over (partition by domain_sessionid order by collector_tstamp) as page_view_index

  from events
  where event = 'page_view'
    and br_family != 'Robot/Spider'
    AND NOT regexp_contains(useragent, '^.*(bot|crawl|slurp|spider|archiv|spinn|sniff|seo|audit|survey|pingdom|worm|capture|(browser|screen)shots|analyz|index|thumb|check|facebook|PingdomBot|PhantomJS|YandexBot|Twitterbot|a_archiver|facebookexternalhit|Bingbot|BingPreview|Googlebot|Baiduspider|360(Spider|User-agent)|semalt).*$')
    and domain_userid is not null
    and domain_sessionidx > 0


),

page_pings as (

  select
    page_view_id,

    min(collector_tstamp) as min_tstamp,
    max(collector_tstamp) as max_tstamp,

    max(doc_width) as doc_width,
    max(doc_height) as doc_height,
    max(br_viewwidth) as br_viewwidth,
    max(br_viewheight) as br_viewheight,
    least(greatest(min(coalesce(pp_xoffset_min, 0)), 0), max(doc_width)) as hmin,
    least(greatest(max(coalesce(pp_xoffset_max, 0)), 0), max(doc_width)) as hmax,
    least(greatest(min(coalesce(pp_yoffset_min, 0)), 0), max(doc_height)) as vmin,
    least(greatest(max(coalesce(pp_yoffset_max, 0)), 0), max(doc_height)) as vmax,

    sum(case when event = 'page_view' then 1 else 0 end) as pv_count,
    sum(case when event = 'page_ping' then 1 else 0 end) as pp_count,

    array_agg(struct(
      event_id,
      event,
      collector_tstamp,
      pp_xoffset_min,
      pp_xoffset_max,
      pp_yoffset_min,
      pp_yoffset_max,
      doc_width,
      doc_height
    ) order by collector_tstamp) as pings_raw

  from events
  where event in ('page_ping', 'page_view')
  group by 1

),

page_pings_relative as (

  select
    page_view_id,

    struct(
      pings_raw as raw,
      struct(
        round(100*(greatest(hmin, 0)/nullif(doc_width, 0))) as relative_hmin,
        round(100*(least(hmax + br_viewwidth, doc_width)/nullif(doc_width, 0))) as relative_hmax,
        round(100*(greatest(vmin, 0)/nullif(doc_height, 0))) as relative_vmin,
        round(100*(least(vmax + br_viewheight, doc_height)/nullif(doc_height, 0))) as relative_vmax,
        min_tstamp,
        max_tstamp,
        pp_count * 10 as time_engaged_in_s
      ) as agg
    ) as pings

  from page_pings

)

select
    page_views.*,
    page_pings_relative.pings,
    coalesce(array_length(forms), 0) as count_form_submissions,
    form_submissions_agg.forms

from page_views
join page_pings_relative using (page_view_id)
left outer join form_submissions_agg using (page_view_id)
