-- Find out how a player's scoring class slowly changed over time
create or replace table test.players_scd
(
	player_name string,
	scoring_class string,
	start_season int64,
	end_season int64,
	current_season int64 -- the year partition on this table
);
-- Find out how a player's scoring class slowly changed over time

select 
    player_name,
    scoring_class,
    min(current_season) as start_season, 
    max(current_season) as end_season, 
    2011 as current_season

from test.players
group by all
order by player_name, scoring_class

-- When new season data arrives, we can run the group by query above all over again to get the latest data. However, it's a waste of resources \
-- since historical data doesnt change for players who didnt join this season or maybe some their scoring class didnt change this season compared to the last season\
-- To solve this, we need to:
    --  1. extract players who played in the last season 
    --  2. extract players who played before but not in the last season, these are historical data that wont change even if they play again this season (considered new players compared to last season)
    --  3. extract players who play in this season and their scoring class 
    --  4. find players who played in last and this season with scoring class changed --> changed_records 
    --  5. find players who played in last and this season but scoring class unchanged --> unchanged_records, update the end_season to this season's end_season
    --  6. find players who didnt play in the last seasons --> new_records
    --  7. combine historical, unchanged_records, changed_records, and new_records we have this season's scd.
-- Essentially we only process the changed and new records --> lesser data. 

with last_season_scd as (
    select 
    *except(current_season) 
    from test.players_scd
    where current_season = 2011 
    and end_season = 2011
), 
historical_scd as (

    select 
    *except(current_season)
    from test.players_scd 
    where current_season = 2011 
    and end_season < 2011
), 
this_season_data as (
    select 
        player_name,
        scoring_class,
        min(current_season) as start_season, 
        max(current_season) as end_season
    from test.players 
    where current_season = 2012 
), 
-- find players with changed scoring class 
changed_records as (
    select 
        ts.player_name, 
        ts.scoring_class,
        ts.start_season, 
        ts.end_season
    from this_season_data ts 
    join last_season_scd ls 
        on ts.player_name = ls.player_name
    where ts.scoring_class <> ls.scoring_class 
), 
-- new players 
new_records as (

    select 
        ts.player_name, 
        ts.scoring_class,
        ts.start_season, 
        ts.end_season
    from this_season_data ts 
    left join last_season_data ls 
        on ts.player_name = ls.player_name 
    where ls.player_name is null 
),
--unchanged_records
unchanged_records as (

    select 

        ls.player_name, 
        ls.scoring_class, 
        ls.start_season,
        ts.end_season -- update end_season

    from  this_season_data ts 
    join last_season_data ls 
        on ts.player_name = ls.player_name 
    where ts.scoring_class = ls.scoring_class 
)
select *, 2012 as current_season from historical_scd
union all by name 
select *, 2012 as current_season from unchanged_records
union all by name 
select *, 2012 as current_season from changed_records
union all by name 
select *, 2012 as current_season from new_records
