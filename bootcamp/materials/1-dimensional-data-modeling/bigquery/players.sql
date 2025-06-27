CREATE or replace TABLE test.players (
  player_name STRING,
  height STRING,
  college STRING,
  country STRING,
  draft_year STRING,
  draft_round STRING,
  draft_number STRING,
  -- Array of structs replaces the custom type array
  season_stats ARRAY<STRUCT<
    season INT64,
    gp INT64,
    pts FLOAT64,
    ast FLOAT64,
    reb FLOAT64
  >>,
  years_since_last_season INT64, 
  scoring_class STRING,
  current_season INT64
)
CLUSTER BY player_name, current_season;

-- Query to populate test.players
-- Each current_season is a complete set of old players with their history and the current game stats if they play this year; and the new players of this season if any.

insert into test.players
with 
yesterday as (
  select
    * 
  from test.players
  where current_season = 1995 -- min(season) -1  from player_seasons, aka the beginning of no data
),
today as (
  select 
  *
  from test.player_seasons
  where season = 1996 -- min(season) = the beginning of data
), 
final as (
select 
  coalesce(t.player_name, y.player_name) as player_name, 
  coalesce(t.height, y.height) as height,
  coalesce(t.college, y.college) as college,
  coalesce(t.country, y.country) as country,
  coalesce(t.draft_year, y.draft_year) as draft_year,
  coalesce(t.draft_round, y.draft_round) as draft_round,
  coalesce(t.draft_number, y.draft_number) as draft_number,

  CASE WHEN y.season_stats IS NULL THEN [STRUCT(t.season, t.gp, t.pts, t.reb, t.ast)] --if yesterday no data, seed this col with today's data
      when t.season is not null then ARRAY_CONCAT(y.season_stats, [STRUCT(t.season, t.gp, t.pts, t.reb, t.ast)]) -- if today has data, append today's data to yesterday's data
      else y.season_stats -- if today no data, keep this col unchanged, aka dont append null array 
  END AS season_stats, 

    -- if player is active this year, set this value to 0, else keep incrementing the value by 1 for every new season. If the player inactive for a while then back, the value is set to 0 again.
  case when t.season is not null then 0 
    else y.years_since_last_season + 1
  end as years_since_last_season, 

  -- the player is scored based on the most recent season they were active
  case when t.season is not null then 
            case when t.pts > 20 then "star"
                when t.pts > 15 then "good"
                when t.pts > 10 then "average"
                else "bad" 
            end 
        else y.scoring_class 
    end as scoring_class,

  coalesce(t.season, y.current_season +1) as current_season -- if this year the player dont play, the current season for that player is incremental by 1 year from his last season, a year partition for this table


from today t 
full outer join yesterday y 
on t.player_name = y.player_name
)
select * from final 

-- get first season pts and latest season pts for each player where current_season = 2000

select 
    player_name, 
    array_first(season_stats).season as first_season,
    array_last(season_stats).season as last_season,
    array_first(season_stats).pts as first_pts, 
    array_last(season_stats).pts as last_pts
from test.players
where current_season = 2000