{{
config(
materialized = 'incremental',
on_schema_change='fail'
)
}}
WITH src_reviews AS (
SELECT * FROM {{ ref('src_reviews') }}
)
SELECT 
{{ dbt_utils.generate_surrogate_key(['listing_id', 'review_date', 'reviewer_name', 'review_text']) }}
AS review_id,
* FROM src_reviews
WHERE review_text is not null
{%  if is_incremental() %}
  {% if var("start_date",FALSE) and var("end_date",False) %}
    {{log('Loading '~this~'Incrementally (start_date: '~var("start_date")~ ', end_date: '~var("end_date")~ ') ',info=True)}}
    AND review_date >='{{var("start_date")}}'
    AND review_date <'{{var("end_date")}}'
  {%else%}
    AND review_date >= coalesce((select max(review_date) from {{ this }}), '1900-01-01')
    {{log('Loading '~this~'Incrementally (all missing dates)',info=True)}}
  {% endif %}
{% endif %}