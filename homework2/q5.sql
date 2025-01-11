-- Drop the `hosts_cumulated` table if it exists
DROP TABLE hosts_cumulated;

-- Create the `hosts_cumulated` table schema to store user host activity
CREATE TABLE hosts_cumulated (
    user_id TEXT,
    host TEXT,
    date DATE,
    host_activity_datelist DATE[],
    PRIMARY KEY (date, user_id, host)
);