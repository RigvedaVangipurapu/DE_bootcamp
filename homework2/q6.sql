-- Generate cumulative `host_activity_datelist` from `events` table
INSERT INTO hosts_cumulated(
WITH yesterday AS (
    SELECT *
    FROM hosts_cumulated
    WHERE date = DATE(CAST('2023-01-02' AS TIMESTAMP))
),
today AS (
    SELECT
        user_id::TEXT,
        host,
        DATE(CAST(event_time AS TIMESTAMP)) AS date_active 
    FROM events e 
    WHERE DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-03')
    AND user_id IS NOT NULL
    AND host IS NOT NULL
    GROUP BY user_id, host, DATE(CAST(event_time AS TIMESTAMP))
)
SELECT 
    COALESCE(t.user_id, y.user_id) AS user_id,
    COALESCE(t.host, y.host) AS host,
    COALESCE(t.date_active, y.date + INTERVAL '1 day') AS date,
    CASE
        WHEN y.host_activity_datelist IS NULL THEN ARRAY[t.date_active]
        WHEN t.date_active IS NULL THEN y.host_activity_datelist
        ELSE ARRAY[t.date_active] || y.host_activity_datelist
    END AS host_activity_datelist
FROM today t
FULL OUTER JOIN yesterday y
ON y.user_id = t.user_id 
AND y.host = t.host
);

-- View the updated records in the `hosts_cumulated` table
SELECT * FROM hosts_cumulated;
