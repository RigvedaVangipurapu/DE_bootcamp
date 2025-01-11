-- Drop the `host_activity_reduced` table if it exists
DROP TABLE host_activity_reduced;

-- Create the `host_activity_reduced` table schema to store monthly aggregated data
CREATE TABLE host_activity_reduced (
    month INTEGER,
    host TEXT,
    hit_array INTEGER[],
    unique_visitors INTEGER[],
    PRIMARY KEY (host, month)
);