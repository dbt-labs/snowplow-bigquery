
select
    event_id,
    form_id

from {{ var('snowplow:context:submit_form') }}
