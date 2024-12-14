-- Select the first 10 rows from the player_seasons table for reference
select * from player_seasons ps limit 10;

-- Create a composite type for season statistics
create type season_stats as
(
	season integer,   -- The season year
	gp integer,       -- Games played
	pts real,         -- Points scored
	reb real,         -- Rebounds
	ast real          -- Assists
);

-- Create an enumerated type for scoring classification
create type scoring_class as enum('star', 'good', 'average', 'bad');

-- Drop the players table if it exists to avoid conflicts
drop table players;

-- Create a new players table with the following schema
create table players 
(
	player_name text,                -- Player's name
	height text,                     -- Player's height
	college text,                    -- College attended by the player
	country text,                    -- Player's country
	draft_year text,                 -- Year player was drafted
	draft_number text,               -- Draft number
	draft_round text,                -- Draft round
	season_stats season_stats[],     -- Array of season statistics
	scoring_class scoring_class,     -- Classification based on scoring performance
	years_since_last_season integer, -- Number of years since the last season played
	current_season integer,          -- The current/latest season
	primary key (player_name, current_season) -- Composite primary key
);

-- Fetch the earliest season available from the player_seasons table
select min(season) from player_seasons ps;

-- Insert data into the players table using a combination of data from yesterday and today. Run with 1995 - 2001 changing yesterday and today
insert into players
with yesterday as 
(
	-- Select data for players who participated in the 2000 season
	select * from players where current_season = 2000
),
today as
(
	-- Select data for players participating in the 2001 season
	select * from player_seasons ps where season = 2001
)
select 
	coalesce(t.player_name, y.player_name) as player_name,  -- Use player name from today's or yesterday's data
	coalesce(t.height, y.height) as height,                -- Carry forward height data
	coalesce(t.college, y.college) as college,             -- Carry forward college data
	coalesce(t.country, y.country) as country,             -- Carry forward country data
	coalesce(t.draft_year, y.draft_year) as draft_year,    -- Carry forward draft year
	coalesce(t.draft_number, y.draft_number) as draft_number, -- Carry forward draft number
	coalesce(t.draft_round, y.draft_round) as draft_round, -- Carry forward draft round
	case 
		when y.season_stats is null then 
			array[row(	
				t.season,
				t.gp,
				t.pts,
				t.reb,
				t.ast) :: season_stats]
		when t.season is not null then
			y.season_stats || 	array[row(	
				t.season,
				t.gp,
				t.pts,
				t.reb,
				t.ast) :: season_stats]
		else 
			y.season_stats
	end as season_stats, -- Update season stats array
	case
		when t.season is not null then 
			case 
				when t.pts > 20 then 'star'
				when t.pts > 15 then 'good'
				when t.pts > 10 then 'average'
				else 'bad'
			end::scoring_class
		else
			y.scoring_class
	end as scoring_class, -- Determine scoring class
	case
		when t.season is not null then 0
		else y.years_since_last_season + 1 
	end as years_since_last_season, -- Update years since last season
	coalesce(t.season, y.current_season + 1) as current_season -- Update current season
from today t
full outer join yesterday y on y.player_name = t.player_name;

-- Unnest season stats for analysis of individual records
with unnested as
(
	select 
		player_name,
		unnest(season_stats)::season_stats as season_stats_flat
	from players where current_season = 2001 
)
select 
	player_name, 
	(season_stats_flat).*
from unnested;

-- Fetch all data for a specific player in the 2000 season
select * from players where current_season  = 2000 and player_name = 'Michael Jordan';

-- Calculate scoring percentage difference from first season to latest season
select 
	player_name,
	(season_stats[1]).pts as first_game, -- Points from the first game
	(season_stats[cardinality(season_stats)]).pts as latest_game, -- Points from the latest game
	(case 
		when (season_stats[1]).pts = 0 then 0  -- Avoid division by zero
		else ((season_stats[cardinality(season_stats)]).pts - (season_stats[1]).pts) * 100 / (season_stats[1]).pts
	end) as percentage -- Calculate percentage change in points
from players
where current_season = 2001;
