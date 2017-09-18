
with all_events as (

  select * from {{ adapter.quote_schema_and_table('snowplow', 'event') }}

),

events as (

    select * from all_events
    {% if target.name == 'dev' %}
        where date(collector_tstamp) > date_sub(current_date, interval 3 day)
    {% endif %}

),

fixed as (

    select
        * except(br_viewheight, event),
        collector_tstamp as derived_tstamp,
        dvce_tstamp as dvce_created_tstamp,
        null as mkt_clickid,
        null as mkt_network,

        case when event = 'pv' then 'page_view'
             when event = 'pp' then 'page_ping'
            else event
        end as event,

        case
            when br_viewheight like '%x%'
            then cast(split(br_viewheight, 'x')[safe_offset(0)] as float64)
            else null
        end as br_viewheight,

        case
            when br_viewheight like '%x%'
            then cast(split(br_viewheight, 'x')[safe_offset(1)] as float64)
            else null
        end as br_viewwidth

    from events
    where
        -- this breaks the snowplow package
        (br_viewheight is null or br_viewheight != 'cliqz.com/tracking')

)

select
    `event_id`,
    `app_id`,
    `br_type`,
    `br_family`,
    `collector_tstamp`,
    `doc_height`,
    `doc_width`,
    `domain_sessionid`,
    `domain_sessionidx`,
    `domain_userid`,
    `dvce_ismobile`,
    `dvce_screenheight`,
    `dvce_sent_tstamp`,
    `dvce_tstamp`,
    `dvce_created_tstamp`,
    `derived_tstamp`,
    `dvce_type`,
    `event`,
    `geo_city`,
    `geo_country`,
    `geo_latitude`,
    `geo_longitude`,
    `geo_region`,
    `geo_region_name`,
    `geo_timezone`,
    `geo_zipcode`,
    `mkt_campaign`,
    `mkt_content`,
    `mkt_medium`,
    `mkt_source`,
    `mkt_term`,
    `os_family`,
    `os_manufacturer`,
    `os_name`,
    `os_timezone`,
    `page_referrer`,
    `page_title`,
    `page_url`,
    `page_urlfragment`,
    `page_urlhost`,
    `page_urlpath`,
    `page_urlport`,
    `page_urlquery`,
    `page_urlscheme`,
    `platform`,
    `pp_xoffset_max`,
    `pp_xoffset_min`,
    `pp_yoffset_max`,
    `pp_yoffset_min`,
    `refr_medium`,
    `refr_source`,
    `refr_term`,
    `refr_urlfragment`,
    `refr_urlhost`,
    `refr_urlpath`,
    `refr_urlport`,
    `refr_urlquery`,
    `refr_urlscheme`,
    `se_action`,
    `se_category`,
    `se_label`,
    `se_property`,
    `se_value`,
    `user_fingerprint`,
    `user_id`,
    `user_ipaddress`,
    `useragent`,
    `v_tracker`,

    -- null as `mkt_clickid`,
    -- null as `mkt_network`,
    -- null as `etl_tstamp`,
    -- null as `dvce_screenwidth`,
    -- null as `ip_isp`,
    -- null as `ip_organization`,
    -- null as `ip_domain`,
    -- null as `ip_netspeed`,
    -- null as `browser`,
    -- null as `browser_name`,
    -- null as `browser_major_version`,
    -- null as `browser_minor_version`,
    -- null as `browser_build_version`,
    -- null as `browser_engine`,
    -- null as `browser_language`,

    case
        when (br_viewwidth) > 100000 then null
        else br_viewwidth
    end as br_viewwidth,

    case
        when (br_viewheight) > 100000 then null
        else br_viewheight
    end as br_viewheight

from fixed
