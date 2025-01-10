-- View data from relevant tables
SELECT * FROM players;
SELECT * FROM players_scd WHERE player_name = 'Allan Houston';
SELECT * FROM player_seasons;

-- Get schema details of the 'players' table
SELECT 
    column_name, 
    data_type, 
    is_nullable, 
    column_default 
FROM information_schema.columns 
WHERE table_name = 'players';

-- Basic query to inspect season stats for players
SELECT 
    player_name,
    season_stats[1] AS season,
    season_stats[2] AS games_played,
    season_stats[3] AS points
FROM players
LIMIT 10;

-- Unnest the season_stats array to access individual stats
SELECT 
    player_name,
    (season_stat).season AS season,
    (season_stat).gp AS games_played,
    (season_stat).pts AS points,
    (season_stat).reb AS rebounds,
    (season_stat).ast AS assists
FROM players p,
LATERAL UNNEST(season_stats) AS season_stat
WHERE (season_stat).season = '2010';

-- Get the min and max seasons across players
SELECT 
    MIN((season_stat).season) AS min_season,
    MAX((season_stat).season) AS max_season
FROM players p,
LATERAL UNNEST(season_stats) AS season_stat;

-- Validate min and max current season values
SELECT 
    MIN(current_season), 
    MAX(current_season) 
FROM players 
WHERE current_season = 2021;

-- Drop and recreate the player_state type
DROP TYPE IF EXISTS player_state;

CREATE TYPE player_state AS ENUM(
    'New', 
    'Retired', 
    'Continued Playing', 
    'Returned from Retirement',
    'Stayed Retired'
);

-- Drop and recreate the player_tracking table
DROP TABLE IF EXISTS player_tracking;

CREATE TABLE player_tracking (
    player_name TEXT,
    season INTEGER,
    start_season INTEGER,
    last_season INTEGER,
    state player_state,
    PRIMARY KEY (player_name, season)
);

-- Insert into player_tracking with logic for state determination
WITH yesterday AS (
    SELECT * 
    FROM player_tracking
    WHERE season = 1997
),
today AS (
    SELECT DISTINCT
        p.player_name,
        p.is_active,
        p.current_season AS season,
        MIN(ps.start_season) AS start_season
    FROM players p
    JOIN LATERAL UNNEST(p.season_stats) AS season_stat ON TRUE
    JOIN players_scd ps ON p.player_name = ps.player_name 
    WHERE p.current_season = 1998
      AND season_stat.season >= ps.start_season
    GROUP BY 
        p.player_name,
        p.is_active,
        p.current_season
)
INSERT INTO player_tracking
SELECT 
    t.player_name,
    t.season,
    COALESCE(y.start_season, t.start_season) AS start_season,
    CASE
        WHEN t.is_active = TRUE AND t.season IS NOT NULL THEN t.season
        ELSE y.last_season
    END AS last_season,
    CASE 
        WHEN t.start_season = t.season THEN 'New'
        WHEN y.start_season < y.season + 1 
             AND y.state IN ('Continued Playing', 'New', 'Returned from Retirement')
             AND t.is_active = FALSE THEN 'Retired'
        WHEN y.state = 'Retired'
             AND t.is_active = TRUE THEN 'Returned from Retirement'
        WHEN y.state = 'Retired'
             AND t.is_active = FALSE THEN 'Stayed Retired'
        ELSE 'Continued Playing'
    END::player_state AS state
FROM today t
FULL OUTER JOIN yesterday y ON y.player_name = t.player_name;

-- Verify the inserted data
SELECT * FROM player_tracking;
