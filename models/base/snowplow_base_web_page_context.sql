
with web_page as (

  select * from {{ var('snowplow:context:web_page') }}

)

select
    event_id,
    id as page_view_id

from web_page
