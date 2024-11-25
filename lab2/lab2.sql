'''step 3 and step 5 are 2 ways of creating SCDs (SCD type 2)
    step 3,4 - load all data at the same time - inefficient since we apply partition on whole data
    step 5 - incremental load after previous SCD - more efficient, more cumbersome
'''

-- STEP 1: Creating the "players" table
-- This table will store player information, including their attributes and career stats.
-- The primary key ensures each player's data is uniquely identified for a given season.

DROP TABLE IF EXISTS players;
CREATE TABLE players (
    player_name TEXT,
    height TEXT,
    college TEXT,
    country TEXT,
    draft_year TEXT,
    draft_number TEXT,
    draft_round TEXT,
    season_stats SEASON_STATS[], -- Array of season stats, custom type
    scoring_class SCORING_CLASS, -- Custom type indicating player scoring level (e.g., star, good)
    years_since_last_season INTEGER, -- Years since the player's last active season
    current_season INTEGER, -- The latest season data
    is_active BOOLEAN, -- Whether the player is active in the current season
    PRIMARY KEY (player_name, current_season)
);

-- STEP 2: Inserting data into "players" table
-- This query generates all player seasons, computes stats, and categorizes players into scoring classes.

INSERT INTO players
WITH years AS (
    -- Generates a series of years from 1996 to 2022 to represent possible seasons.
    SELECT * FROM GENERATE_SERIES(1996, 2022) AS season
),
p AS (
    -- Identifies the first season for each player.
    SELECT player_name, MIN(season) AS first_season
    FROM player_seasons
    GROUP BY player_name
),
players_and_seasons AS (
    -- Creates a list of all seasons for each player starting from their first season.
    SELECT * 
    FROM p 
    JOIN years y ON p.first_season <= y.season
),
windowed AS (
    -- Aggregates season stats into an array for each player across seasons.
    SELECT
        pas.player_name,
        pas.season,
        ARRAY_REMOVE(
            ARRAY_AGG(
                CASE
                    WHEN ps.season IS NOT NULL
                    THEN ROW(ps.season, ps.gp, ps.pts, ps.reb, ps.ast)::season_stats
                END
            ) OVER (PARTITION BY pas.player_name ORDER BY COALESCE(pas.season, ps.season)),
            NULL
        ) AS seasons
    FROM players_and_seasons pas
    LEFT JOIN player_seasons ps
        ON pas.player_name = ps.player_name AND pas.season = ps.season
    ORDER BY pas.player_name, pas.season
),
static AS (
    -- Extracts static player attributes such as height, college, and draft info.
    SELECT
        player_name,
        MAX(height) AS height,
        MAX(college) AS college,
        MAX(country) AS country,
        MAX(draft_year) AS draft_year,
        MAX(draft_round) AS draft_round,
        MAX(draft_number) AS draft_number
    FROM player_seasons
    GROUP BY player_name
)
SELECT
    w.player_name,
    s.height,
    s.college,
    s.country,
    s.draft_year,
    s.draft_round,
    s.draft_number,
    seasons AS season_stats,
    -- Assigns a scoring class to each player based on their performance stats.
    CASE
        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 20 THEN 'star'
        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 15 THEN 'good'
        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 10 THEN 'average'
        ELSE 'bad'
    END::scoring_class AS scoring_class,
    -- Calculates years since the player's last active season.
    w.season - (seasons[CARDINALITY(seasons)]::season_stats).season AS years_since_last_active,
    w.season,
    -- Determines if the player is active in the current season.
    (seasons[CARDINALITY(seasons)]::season_stats).season = season AS is_active
FROM windowed w
JOIN static s
    ON w.player_name = s.player_name;

-- View the data inserted into the "players" table.
SELECT * FROM players;

-- STEP 3: Creating the "players_scd" table
-- This table will store Slowly Changing Dimensions (SCD) for players,
-- tracking changes in scoring class and activity status over time.

DROP TABLE IF EXISTS players_scd;
CREATE TABLE players_scd (
    player_name TEXT,
    scoring_class SCORING_CLASS,
    is_active BOOLEAN,
    current_season INTEGER,
    start_season INTEGER,
    end_season INTEGER,
    PRIMARY KEY (player_name, start_season)
);

-- STEP 4: Insert SCD data into "players_scd"
-- This query identifies streaks of consistent scoring class and activity for each player.

INSERT INTO players_scd
WITH with_previous AS (
    -- Compares current season's scoring class and activity status with the previous season's data.
    SELECT 
        player_name, 
        scoring_class, 
        is_active,
        current_season,
        LAG(scoring_class, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_scoring_class,
        LAG(is_active, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_is_active
    FROM players p
    WHERE current_season <= 2021 -- Only consider seasons up to 2021.
),
with_indicators AS (
    -- Adds a change indicator (1 if scoring class or activity changed, 0 otherwise).
    SELECT
        *,
        CASE 
            WHEN scoring_class != previous_scoring_class THEN 1 
            WHEN is_active != previous_is_active THEN 1 
            ELSE 0
        END AS change_indicator
    FROM with_previous
),
with_streaks AS (
    -- Groups consecutive seasons with the same scoring class and activity status.
    SELECT *,
        SUM(change_indicator)
        OVER (PARTITION BY player_name ORDER BY current_season) AS streak_identifier
    FROM with_indicators
)
-- Inserts aggregated streak data (start and end season for each streak).
SELECT
    player_name,
    scoring_class,
    is_active,
    2021 AS current_season,
    MIN(current_season) AS start_season,
    MAX(current_season) AS end_season
FROM with_streaks
GROUP BY 
    player_name,
    streak_identifier,
    is_active,
    scoring_class
ORDER BY player_name, start_season;

-- View the inserted SCD data.
SELECT * FROM players_scd;

-- STEP 5: Handling updates for 2022
-- This block identifies unchanged, changed, and new records for the 2022 season,
-- updating the players_scd table accordingly.

-- Define a custom type for holding SCD record changes.
CREATE TYPE scd_type AS (
    scoring_class SCORING_CLASS,
    is_active BOOLEAN,
    start_season INTEGER,
    end_season INTEGER
);

WITH last_season_scd AS (
    -- Selects SCD records that ended in 2021.
    SELECT * 
    FROM players_scd
    WHERE current_season = 2021
    AND end_season = 2021
),
historical_scd AS (
    -- Selects historical SCD records that ended before 2021.
    SELECT 
        player_name,
        scoring_class,
        is_active,
        start_season,
        end_season
    FROM players_scd
    WHERE current_season = 2021
    AND end_season < 2021
),
this_season_data AS (
    -- Gets player data for the 2022 season.
    SELECT * 
    FROM players 
    WHERE current_season = 2022
),
unchanged_records AS (
    -- Identifies records that remain unchanged in 2022.
    SELECT 
        ts.player_name,
        ts.scoring_class,
        ts.is_active,
        ls.start_season,
        ts.current_season AS end_season
    FROM this_season_data ts
    JOIN last_season_scd ls ON ls.player_name = ts.player_name
    WHERE ts.scoring_class = ls.scoring_class AND ts.is_active = ls.is_active
),
changed_records AS (
    -- Identifies records that changed in 2022 and splits them into two parts.
    SELECT 
        ts.player_name,
        UNNEST(ARRAY[
            ROW(ls.scoring_class, ls.is_active, ls.start_season, ls.end_season)::scd_type,
            ROW(ts.scoring_class, ts.is_active, ts.current_season, ts.current_season)::scd_type
        ]) AS records
    FROM this_season_data ts
    LEFT JOIN last_season_scd ls ON ls.player_name = ts.player_name
    WHERE ts.scoring_class != ls.scoring_class OR ts.is_active != ls.is_active
),
unnested_changed_records AS (
    -- Unnests changed records for insertion.
    SELECT 
        player_name,
        (records).scoring_class,
        (records).is_active,
        (records).start_season,
        (records).end_season
    FROM changed_records
),
new_records AS (
    -- Identifies completely new records for players who didn't exist in 2021.
    SELECT 
        ts.player_name,
        ts.scoring_class,
        ts.is_active,
        ts.current_season AS start_season,
        ts.current_season AS end_season
    FROM this_season_data ts
    LEFT JOIN last_season_scd ls ON ls.player_name = ts.player_name
    WHERE ls.player_name IS NULL
)
-- Combines all historical, unchanged, changed, and new records into one result set.
SELECT * FROM historical_scd
union all
select * from unchanged_records
union all
select * from unnested_changed_records
union all
select * from new_records;




