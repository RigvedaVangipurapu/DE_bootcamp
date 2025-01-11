-- Deduplicate the `game_details` table by selecting distinct rows
SELECT DISTINCT * FROM game_details gd;

-- Drop the `user_devices_cumulated` table if it exists
DROP TABLE user_devices_cumulated;

-- Create the `user_devices_cumulated` table schema to store user device activity
CREATE TABLE user_devices_cumulated (
    user_id TEXT,
    browser_type TEXT,
    date DATE,
    device_activity_datelist DATE[],
    PRIMARY KEY (date, user_id, browser_type)
);