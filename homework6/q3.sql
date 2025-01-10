select * from game_details gd where gd.player_name like 'LeBron%';
select * from games;
select * from players:

WITH team_game_results AS (
    SELECT 
        g.game_id,
        CASE 
            WHEN g.home_team_wins = 1 THEN g.home_team_id
            ELSE g.visitor_team_id
        END AS winning_team_id
    FROM games g
),
team_win_counts AS (
    SELECT 
        winning_team_id AS team_id,
        ROW_NUMBER() OVER (PARTITION BY winning_team_id ORDER BY g.game_date_est) AS game_number,
        COUNT(*) OVER (PARTITION BY winning_team_id ORDER BY g.game_date_est ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) AS wins_in_90_games
    FROM team_game_results tgr
    JOIN games g ON tgr.game_id = g.game_id
)
SELECT 
    team_id,
    MAX(wins_in_90_games) AS most_wins_in_90_games
FROM team_win_counts
GROUP BY team_id
ORDER BY most_wins_in_90_games DESC
LIMIT 1;


WITH scoring_data AS (
    SELECT 
        player_id,
        game_date_est as game_date,
        CASE 
            WHEN pts > 10 THEN 1 
            ELSE 0 
        END AS scored_above_10,
        ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY game_date_est) AS row_num,
        SUM(CASE WHEN pts > 10 THEN 1 ELSE 0 END) 
            OVER (PARTITION BY player_id ORDER BY game_date_est) AS cumulative_sum
    FROM game_details gd
    join games g on g.game_id = gd.game_id
    WHERE player_id = 2544 -- Filter for LeBron or adjust as needed
),
streaks AS (
    SELECT 
        player_id,
        game_date,
        scored_above_10,
        row_num - cumulative_sum AS streak_group
    FROM scoring_data
)
SELECT 
    player_id,
    MIN(game_date) AS streak_start_date,
    MAX(game_date) AS streak_end_date,
    COUNT(*) AS streak_length
FROM streaks
WHERE scored_above_10 = 1
GROUP BY player_id, streak_group
ORDER BY streak_length DESC
LIMIT 1;
--62 games
