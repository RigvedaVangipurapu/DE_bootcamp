-- Define the vertex types (player, team, game) for the graph structure.
-- This will categorize vertices in the graph database to represent players, teams, and games.

-- Drop the vertex_type enum if it exists, along with any dependent objects
drop type vertex_type cascade;

-- Create an enum type to categorize vertices as 'player', 'team', or 'game'
create type vertex_type as enum ('player', 'team', 'game');

-- Define the edges table and its relationship types (plays_in, plays_against, shares_team, plays_on).
-- These will represent how vertices (e.g., players and games) relate to one another.

-- Drop the vertices table if it exists
drop table if exists vertices;

-- Create the vertices table to store nodes of the graph
create table vertices
(
	identifier text,                  -- Unique identifier for the vertex
	type vertex_type,                 -- Type of the vertex (player, team, or game)
	properties json,                  -- Additional properties as JSON
	primary key (identifier, type)    -- Composite primary key
);

-- Drop the edge_type enum if it exists, along with any dependent objects
drop type edge_type cascade;

-- Create an enum type to categorize edges
create type edge_type as enum ('plays_against', 'shares_team', 'plays_in', 'plays_on');

-- Drop the edges table if it exists
drop table if exists edges;

-- Create the edges table to store relationships between vertices
create table edges
(
	subject_identifier text,          -- Identifier for the subject vertex
	subject_type vertex_type,         -- Type of the subject vertex
	object_identifier text,           -- Identifier for the object vertex
	object_type vertex_type,          -- Type of the object vertex
	edge_type edge_type,              -- Type of the edge (relationship)
	properties json,                  -- Additional properties as JSON
	primary key (subject_identifier, subject_type, object_identifier, object_type, edge_type)
);

-- View the games table for reference to understand the data being processed.
-- This ensures we understand the data being used to populate the graph.

-- Select all records from the games table (example query)
select * from games;

-- Populate the vertices table with game details.
-- Each game will be represented as a vertex with JSON properties capturing game-specific data.

insert into vertices
select 
	game_id as identifier,            -- Game ID as the vertex identifier
	'game'::vertex_type as type,      -- Assign vertex type as 'game'
	json_build_object(                -- Build JSON properties
		'pts_home', pts_home,
		'pts_away', pts_away,
		'winning_team', 
		case when home_team_wins = 1 then home_team_id else visitor_team_id end
	) as properties
from games;

-- Populate the vertices table with player details.
-- Each player will be represented as a vertex with JSON properties capturing aggregated stats.

insert into vertices
with players_agg as (
	select 
		player_id as identifier,       -- Player ID as the vertex identifier
		max(player_name) as player_name,
		count(1) as number_of_games,   -- Total games played by the player
		sum(pts) as total_points,      -- Total points scored by the player
		array_agg(distinct team_id) as teams -- List of teams the player has been part of
	from game_details 
	group by player_id 
)
select 
	identifier, 
	'player'::vertex_type,            -- Assign vertex type as 'player'
	json_build_object(                -- Build JSON properties
		'player_name', player_name,
		'number_of_games', number_of_games,
		'total_points', total_points,
		'teams', teams
	)
from players_agg;

-- Populate the vertices table with team details.
-- Each team will be represented as a vertex with JSON properties capturing its details.

insert into vertices
with teams_deduped as (
	select *, row_number() over (partition by team_id) as row_num
	from teams
)
select 
	team_id as identifier,            -- Team ID as the vertex identifier
	'team'::vertex_type as type,      -- Assign vertex type as 'team'
	json_build_object(                -- Build JSON properties
		'abbreviation', abbreviation,
		'nickname', nickname,
		'city', city,
		'arena', arena,
		'year_founded', yearfounded
	) 
from teams_deduped
where row_num = 1;                  -- Deduplicate by taking the first record per team

-- Verify the data inserted into the vertices table.
select * from vertices;
select type, count(1) from vertices group by 1;

-- Populate the edges table with 'plays_in' relationships.
-- This connects players to the games they played in, with additional game-specific details.

insert into edges
with deduped as (
	select *, row_number() over(partition by player_id, game_id) as row_num 
	from game_details
)
select 
	player_id as subject_identifier, -- Player as the subject vertex
	'player'::vertex_type as subject_type,
	game_id as object_identifier,    -- Game as the object vertex
	'game'::vertex_type as object_type,
	'plays_in'::edge_type as edge_type, -- Relationship type
	json_build_object(               -- Build JSON properties
		'start_position', start_position,
		'pts', pts,
		'team_id', team_id,
		'team_abbreviation', team_abbreviation
	) as properties
from deduped 
where row_num = 1;                  -- Deduplicate by taking the first record

-- Query the maximum points scored by each player in a single game.
select 	
	v.properties ->> 'player_name',   -- Get player name from vertex properties
	max(cast(e.properties ->> 'pts' as integer)) -- Maximum points scored in a game
from vertices v
join edges e on e.subject_identifier = v.identifier
and e.subject_type = v.type
group by 1
order by 2 desc;

-- Populate the edges table with player-to-player relationships.
-- Relationships include 'shares_team' (same team) and 'plays_against' (opposite teams).
-- Duplicate edges (e.g., A-B and B-A) are avoided.

insert into edges
with deduped as (
	select *, row_number() over(partition by player_id, game_id) as row_num 
	from game_details
),
filtered as (
	select * from deduped where row_num = 1
),
aggregated as (
	select 
		f1.player_id as subject_player_id,
		f2.player_id as object_player_id,
		max(f1.player_name) as subject_player_name,
		max(f2.player_name) as object_player_name,
		case when f1.team_abbreviation = f2.team_abbreviation 
		     then 'shares_team'::edge_type 
		     else 'plays_against'::edge_type
		end as edge_type,
		count(1) as num_games,          -- Number of games the players shared
		sum(f1.pts) as left_points,     -- Points scored by subject player
		sum(f2.pts) as right_points     -- Points scored by object player
	from filtered f1
	join filtered f2 
	on f1.game_id = f2.game_id 
	and f1.player_name > f2.player_name -- Remove duplicate edges by comparing names
	group by 
		f1.player_id,
		f2.player_id,
		case when f1.team_abbreviation = f2.team_abbreviation 
		     then 'shares_team'::edge_type 
		     else 'plays_against'::edge_type
		end
)
select 	
	subject_player_id as subject_identifier,
	'player'::vertex_type as subject_type,
	object_player_id as object_identifier,
	'player'::vertex_type as object_type,
	edge_type,
	json_build_object(
		'num_games', num_games,
		'subject_points', left_points,
		'object_points', right_points
	) as properties
from aggregated;

-- Calculate average points for each player in his overall matches and compare it with stats per game for players and retrieve other relevant details.
select 
	v.properties ->> 'player_name',  -- Player name
	e.object_identifier,             -- Opponent/player ID
	cast(v.properties ->> 'total_points' as real) /
	case when cast(v.properties ->> 'number_of_games' as real) = 0 
	then 1 
	else cast(v.properties ->> 'number_of_games' as real) 
	end as avg_points,              -- Calculate average points per game
	e.properties->>'subject_points', -- Points scored in this relationship
	e.properties ->>'num_games'     -- Number of games in this relationship
from vertices v
join edges e 
on v.identifier = e.subject_identifier  
and v.type = e.subject_type 
where e.object_type = 'player'::vertex_type;
