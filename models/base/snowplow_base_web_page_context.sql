
with web_page as (

  select * from {{ adapter.quote_schema_and_table('snowplow', 'web_page') }}

)

select
    event_id,
    id as page_view_id

from web_page
