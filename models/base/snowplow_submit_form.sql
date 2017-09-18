
select
    event_id,
    form_id

from {{ adapter.quote_schema_and_table('snowplow', 'submit_form') }}
