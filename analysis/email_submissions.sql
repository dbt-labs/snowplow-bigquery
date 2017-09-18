
select distinct
  page_view_id,
  elements.value

from {{ target.schema }}.`snowplow_page_views*`
join unnest(forms) as forms
join unnest(forms.elements) as elements
where count_form_submissions > 0
  and elements.name = 'email'
