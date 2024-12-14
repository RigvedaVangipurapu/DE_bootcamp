select * from game_details gd ;

-- grain of the table 
select
	game_id, team_id, player_id, count(1)
from game_details gd 
group by game_id , team_id , player_id 
having count(1)>1;

-- there are duplicates - could be generated while logging
-- we need to create a filter and dedupe it

with deduped as
(
	select
		g.game_date_est
		, g.home_team_id
		, g.season
		, gd.*
		, row_number () over (partition by gd.game_id, gd.team_id, gd.player_id order by g.game_date_est) as row_num
		from game_details gd 
		join games g on g.game_id = gd.game_id
)
select 
	game_date_est,
	season,
	team_id,
	team_id = home_team_id as dim_is_playing_at_home,
	player_id,
	player_name,
	start_position,
	coalesce (position('DNP' in comment),0)>0 as dim_did_not_play,
	coalesce (position('DND' in comment),0)>0 as dim_did_not_dress,
	coalesce (position('NWT' in comment),0)>0 as dim_not_with_team,
--	comment,
	split_part(min,':',1)::float + (split_part(min, ':',2)::float)/60 as minutes,
	fgm, -- field goals made
	fga, --field goals attempted
	fg3m, --3 pointers made
	fg3a, --3 pointers attempted
	ftm, -- free throws made
	fta, --free throws attemted
	--rebounds
	reb,
	dreb,
	oreb,
	ast, --assist
	stl, --steal
	blk, --block
	"TO" as turnovers,
	pf, --personal fouls
	pts, --points
	plus_minus
from deduped where row_num = 1
;

create table fact_game_details(
	dim_game_date date,
	dim_season integer,
	dim_is_playing_at_home boolean,
	dim_team_id integer,
	dim_player_id integer,
	dim_player_name text,
	dim_start_position text,
	dim_did_not_play boolean,
	dim_did_not_dress boolean,
	dim_not_with_team boolean,
	m_minutes real,
	m_fgm integer,
	m_fga integer,
	m_fg3m integer,
	m_fg3a integer,
	m_ftm integer,
	m_fta integer,
	m_dreb integer,
	m_oreb integer,
	m_reb integer,
	m_ast integer,
	m_stl integer,
	m_blk integer,
	m_turnovers integer,
	m_pf integer,
	m_pts integer,
	m_plus_minus integer,
	primary key (dim_game_date, dim_player_id, dim_team_id)
);

insert into fact_game_details(
WITH deduped AS (
    SELECT
        g.game_date_est,
        g.home_team_id,
        g.season,
        gd.*,
        ROW_NUMBER() OVER (PARTITION BY gd.game_id, gd.team_id, gd.player_id ORDER BY g.game_date_est) AS row_num
    FROM game_details gd
    JOIN games g ON g.game_id = gd.game_id
)
SELECT
    game_date_est AS dim_game_date,
    season AS dim_season,
    team_id = home_team_id AS dim_is_playing_at_home,
    team_id AS dim_team_id,
    player_id AS dim_player_id,
    player_name AS dim_player_name,
    start_position AS dim_start_position,
    COALESCE(POSITION('DNP' IN comment) > 0, false) AS dim_did_not_play,
    COALESCE(POSITION('DND' IN comment) > 0, false) AS dim_did_not_dress,
    COALESCE(POSITION('NWT' IN comment) > 0, false) AS dim_not_with_team,
	split_part(min,':',1)::float + (split_part(min, ':',2)::float)/60 AS m_minutes,
    fgm AS m_fgm,
    fga AS m_fga,
    fg3m AS m_fg3m,
    fg3a AS m_fg3a,
    ftm AS m_ftm,
    fta AS m_fta,
    dreb AS m_dreb,
    oreb AS m_oreb,
    reb AS m_reb,
    ast AS m_ast,
    stl AS m_stl,
    blk AS m_blk,
    "TO" AS m_turnovers,
    pf AS m_pf,
    pts AS m_pts,
    plus_minus AS m_plus_minus
FROM deduped
WHERE row_num = 1
);

select * from fact_game_details;