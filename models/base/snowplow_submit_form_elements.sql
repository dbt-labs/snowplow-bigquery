
select
    _event_id as event_id,
    _index as index,
    name,
    value

from {{ var('snowplow:context:submit_form:elements') }}
