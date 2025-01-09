-- Deduplicate events table to filter only relevant events ('/signup', '/api/v1/login') 
-- and ensure non-null user IDs.
WITH deduped_events AS (
    SELECT 
        user_id, 
        url, 
        event_time, 
        DATE(event_time) AS event_date
    FROM events
    WHERE url IN ('/signup', '/api/v1/login')
      AND user_id IS NOT NULL
),

-- Perform a self-join to identify sequences of events by the same user on the same day.
selfjoined AS (
    SELECT 
        d1.user_id,
        d1.event_time,
        d1.url,
        d2.event_time AS destination_event_time,
        d2.url AS destination_url
    FROM deduped_events d1
    JOIN deduped_events d2 
        ON d2.user_id = d1.user_id 
       AND d1.event_date = d2.event_date
       AND d2.event_time >= d1.event_time
    -- Uncomment the lines below to filter for specific sequences, e.g., signup followed by login.
    -- WHERE d1.url = '/signup'
    --   AND d2.url = '/api/v1/login'
),

-- Aggregate user-level data to calculate the number of hits and whether the user converted (visited '/api/v1/login').
userlevel AS (
    SELECT 
        user_id, 
        url,
        COUNT(1) AS number_of_hits,
        MAX(CASE WHEN destination_url = '/api/v1/login' THEN 1 ELSE 0 END) AS converted
    FROM selfjoined sj
    GROUP BY user_id, url
)

-- Calculate summary metrics at the URL level, including the total number of hits,
-- total conversions, and the percentage of users who converted for each URL.
SELECT 
    url, 
    COUNT(1) AS user_count, 
    SUM(number_of_hits) AS total_hits, 
    CAST(SUM(converted) AS REAL) / COUNT(1) AS pct_converted
FROM userlevel
GROUP BY url;

-- Identify all distinct URLs present in the events table.
SELECT DISTINCT url FROM events;

-- Create a dashboard table to analyze device hits using GROUPING SETS for multiple aggregation levels.
CREATE TABLE device_hits_dashboard AS

-- Augment the events table with device information, using COALESCE to handle null values.
WITH events_augmented AS (
    SELECT 
        COALESCE(d.os_type, 'unknown')      AS os_type,
        COALESCE(d.device_type, 'unknown')  AS device_type,
        COALESCE(d.browser_type, 'unknown') AS browser_type,
        url,
        user_id
    FROM events e
    JOIN devices d ON e.device_id = d.device_id
)

-- Perform aggregations at multiple levels using GROUPING SETS to analyze hits by os_type, device_type, and browser_type.
SELECT
    CASE
        WHEN GROUPING(os_type) = 0
         AND GROUPING(device_type) = 0
         AND GROUPING(browser_type) = 0 THEN 'os_type__device_type__browser'
        WHEN GROUPING(browser_type) = 0 THEN 'browser_type'
        WHEN GROUPING(device_type) = 0 THEN 'device_type'
        WHEN GROUPING(os_type) = 0 THEN 'os_type'
    END AS aggregation_level,
    COALESCE(os_type, '(overall)') AS os_type,
    COALESCE(device_type, '(overall)') AS device_type,
    COALESCE(browser_type, '(overall)') AS browser_type,
    COUNT(1) AS number_of_hits
FROM events_augmented
GROUP BY GROUPING SETS (
    (browser_type, device_type, os_type),
    (browser_type),
    (os_type),
    (device_type)
)
ORDER BY COUNT(1) DESC;
