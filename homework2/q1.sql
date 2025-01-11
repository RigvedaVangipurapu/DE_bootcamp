-- Retrieve all records from the `game_details` table
SELECT * FROM game_details gd;

-- Retrieve all records from the `events` table
SELECT * FROM events;

-- Retrieve all records from the `devices` table
SELECT * FROM devices;

-- Identify duplicate records in `game_details` by game_id, team_id, and player_id
SELECT 
    game_id, team_id, player_id, COUNT(*)
FROM game_details gd 
GROUP BY game_id, team_id, player_id 
HAVING COUNT(*) > 1;

-- Fetch specific duplicate records in `game_details` for a particular combination
SELECT * FROM game_details gd 
WHERE 
    game_id = 22000071 AND team_id = 1610612742 AND player_id = 1630179;

-- Deduplicate the `game_details` table by selecting distinct rows
SELECT DISTINCT * FROM game_details gd;