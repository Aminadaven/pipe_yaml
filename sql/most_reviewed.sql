select taster_name, count(*) as ct_reviews, {{ ENV_CODE }} as code
from {{ winemag_df|default('winemag_data') }}
where taster_name is not null
group by taster_name
order by ct_reviews desc
limit 1