
select
    _event_id as event_id,
    _index as index,
    name,
    value

from {{ adapter.quote_schema_and_table('snowplow', 'submit_form_elements') }}
