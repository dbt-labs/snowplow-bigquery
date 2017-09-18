
{{ config(materialized='table') }}

with forms as (

    select * from {{ ref('snowplow_submit_form') }}

),

classes as (

    select * from {{ ref('snowplow_submit_form_classes') }}

),

elements as (

    select * from {{ ref('snowplow_submit_form_elements') }}

),

page_views as (

  select event_id, page_view_id from {{ ref('snowplow_base_web_page_context') }}

),

elements_xf as (

  select
    event_id,
    array_agg(struct(
      index,
      name,
      value
      )
      order by index
    ) as elements
  from elements
  group by 1

),

classes_xf as (

    select
        event_id,
        array_agg(struct(index, class) order by index) as classes
    from classes
    group by 1

)

select *
from forms
join page_views using (event_id)
join classes_xf using (event_id)
join elements_xf using (event_id)
