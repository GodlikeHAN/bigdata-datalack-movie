with base as (
    select * from {{ ref('fct_movie_performance') }}
)

select
    release_year,
    coalesce(list_extract(genres, 1), 'Unknown') as main_genre,
    count(*) as movie_count,
    avg(rating_consensus_score) as avg_rating_consensus_score,
    avg(revenue) as avg_revenue,
    avg(budget) as avg_budget,
    avg(roi) as avg_roi,
    avg(commercial_score - rating_consensus_score) as avg_performance_gap
from base
group by 1, 2
