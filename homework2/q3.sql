-- Generate cumulative `device_activity_datelist` from `events` and `devices` tables
INSERT INTO user_devices_cumulated(
WITH user_browser_deduped AS (
    SELECT 
        e.user_id::TEXT,
        e.event_time,
        MAX(d.browser_type) AS browser_type
    FROM events e 
    JOIN devices d ON d.device_id = e.device_id
    WHERE e.user_id IS NOT NULL
    AND d.browser_type IS NOT NULL
    GROUP BY e.user_id, e.event_time
),
yesterday AS (
    SELECT *
    FROM user_devices_cumulated
    WHERE date = DATE(CAST('2023-01-02' AS TIMESTAMP))
),
today AS (
    SELECT
        user_id,
        browser_type,
        DATE(CAST(event_time AS TIMESTAMP)) AS date_active 
    FROM user_browser_deduped ub
    WHERE DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-03')
    GROUP BY user_id, browser_type, DATE(CAST(event_time AS TIMESTAMP))
)
SELECT 
    COALESCE(t.user_id, y.user_id) AS user_id,
    COALESCE(t.browser_type, y.browser_type) AS browser_type,
    COALESCE(t.date_active, y.date + INTERVAL '1 day') AS date,
    CASE
        WHEN y.device_activity_datelist IS NULL THEN ARRAY[t.date_active]
        WHEN t.date_active IS NULL THEN y.device_activity_datelist
        ELSE ARRAY[t.date_active] || y.device_activity_datelist
    END AS device_activity_datelist
FROM today t
FULL OUTER JOIN yesterday y
ON y.user_id = t.user_id 
AND y.browser_type = t.browser_type
);

-- View the updated records in the `user_devices_cumulated` table
SELECT * FROM user_devices_cumulated;