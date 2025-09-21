select taster_name, count(*) as ct_reviews
from {{ winemag_df }}
where taster_name is not null
group by taster_name
order by ct_reviews desc
limit 1