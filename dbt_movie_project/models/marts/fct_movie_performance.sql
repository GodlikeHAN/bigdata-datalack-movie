with tmdb as (
    select * from {{ ref('stg_tmdb_movies') }}
),
omdb as (
    select * from {{ ref('stg_omdb_ratings') }}
)

select
    tmdb.tmdb_id,
    tmdb.imdb_id,
    tmdb.title,
    tmdb.release_year,
    tmdb.release_date,
    tmdb.genres,
    tmdb.runtime,
    tmdb.budget,
    tmdb.revenue,
    tmdb.revenue - tmdb.budget as profit,
    case when tmdb.budget > 0 then (tmdb.revenue - tmdb.budget) * 1.0 / tmdb.budget end as roi,
    tmdb.tmdb_vote_average,
    tmdb.tmdb_score_100,
    omdb.imdb_rating,
    omdb.imdb_score_100,
    omdb.rt_score_100,
    omdb.metacritic_score_100,
    (
      coalesce(tmdb.tmdb_score_100 * 0.35, 0)
      + coalesce(omdb.imdb_score_100 * 0.35, 0)
      + coalesce(omdb.rt_score_100 * 0.20, 0)
      + coalesce(omdb.metacritic_score_100 * 0.10, 0)
    ) / nullif(
      case when tmdb.tmdb_score_100 is not null then 0.35 else 0 end
      + case when omdb.imdb_score_100 is not null then 0.35 else 0 end
      + case when omdb.rt_score_100 is not null then 0.20 else 0 end
      + case when omdb.metacritic_score_100 is not null then 0.10 else 0 end,
      0
    ) as rating_consensus_score,
    tmdb.popularity,
    ntile(100) over (order by tmdb.revenue nulls first) * 0.45
      + ntile(100) over (order by case when tmdb.budget > 0 then (tmdb.revenue - tmdb.budget) * 1.0 / tmdb.budget end nulls first) * 0.35
      + ntile(100) over (order by tmdb.popularity nulls first) * 0.20 as commercial_score
from tmdb
left join omdb
  on tmdb.imdb_id = omdb.imdb_id
