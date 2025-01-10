-- Query 1: Player with the Most Points in a Season
WITH player_season_agg AS (
    SELECT 
        CASE
            WHEN GROUPING(gd.player_id) = 1 THEN '(overall)'
            ELSE gd.player_id::text
        END AS player,
        CASE
            WHEN GROUPING(g.season) = 1 THEN '(overall)'
            ELSE g.season::text
        END AS season,
        SUM(gd.pts) AS total_points
    FROM game_details gd
    JOIN games g ON g.game_id = gd.game_id
    GROUP BY GROUPING SETS (
        (player_id, season), 
        (player_id),         
        (season),            
        ()                   
    )
),
player_season_result AS (
    SELECT 
        player,
        MAX(total_points) AS max_points
    FROM player_season_agg
    WHERE total_points IS NOT NULL
    GROUP BY player
)
SELECT 
    player,
    max_points
FROM player_season_result
ORDER BY max_points DESC;
--answer: 201935

-- Query 2: Player with the Most Points for One Team
WITH player_team_agg AS (
    SELECT 
        CASE
            WHEN GROUPING(player_id) = 1 THEN '(overall)'
            ELSE player_id::text
        END AS player,
        CASE
            WHEN GROUPING(team_id) = 1 THEN '(overall)'
            ELSE team_id::text
        END AS team,
        SUM(pts) AS total_points
    FROM game_details gd
    GROUP BY GROUPING SETS (
        (player_id, team_id), 
        (player_id),         
        (team_id),            
        ()                   
    )
),
player_team_result AS (
    SELECT 
        player,
        MAX(total_points) AS max_points
    FROM player_team_agg
    WHERE total_points IS NOT NULL
    GROUP BY player
)
SELECT 
    player,
    max_points
FROM player_team_result
ORDER BY max_points DESC;
--answer: 201935

-- Query 3: Team with the Most Wins
WITH team_scores AS (	
    SELECT 	
        CASE 
            WHEN home_team_wins = 1 THEN home_team_id 
            ELSE visitor_team_id 
        END AS team_id,
        COUNT(1) AS wins
    FROM games g
    JOIN game_details gd ON gd.game_id = g.game_id
    GROUP BY CASE 
        WHEN home_team_wins = 1 THEN home_team_id 
        ELSE visitor_team_id 
    END
),
team_wins_agg AS (
    SELECT 
        CASE
            WHEN GROUPING(team_id) = 1 THEN '(overall)'
            ELSE team_id::text
        END AS team,
        SUM(wins) AS total_wins
    FROM team_scores
    GROUP BY GROUPING SETS (       
        (team_id),          
        ()                  
    )
)
SELECT 
    team,
    total_wins
FROM team_wins_agg
ORDER BY total_wins DESC;
--answer: 1610612744