select * from game_details gd where gd.player_name like 'LeBron%';
select * from games;
select * from players:

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
