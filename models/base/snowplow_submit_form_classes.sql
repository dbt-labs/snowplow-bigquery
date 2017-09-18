

select
    _event_id as event_id,
    _index as index,
    form_classes as class

from {{ adapter.quote_schema_and_table('snowplow', 'submit_form_form_classes') }}
