-- Generate and upsert monthly aggregated data for `host_activity_reduced`
WITH yesterday AS (
    SELECT * 
    FROM host_activity_reduced
    WHERE month = EXTRACT(MONTH FROM DATE '2023-01-01')
),
today AS (
    SELECT 
        EXTRACT(MONTH FROM date::timestamp) AS month,
        date,
        host,
        COUNT(
            CASE 
                WHEN host_activity_datelist @> ARRAY[DATE(date)] THEN 1 
            END
        ) AS num_hits,
        COUNT(DISTINCT 
            CASE 
                WHEN host_activity_datelist @> ARRAY[DATE(date)] THEN user_id 
            END
        ) AS unique_users
    FROM hosts_cumulated
    WHERE date = DATE '2023-01-02'
    GROUP BY     
        EXTRACT(MONTH FROM date::timestamp),
        date,
        host
),
combined AS (
    SELECT
        COALESCE(t.month, y.month) AS month,
        COALESCE(t.host, y.host) AS host,
        CASE
            WHEN t.num_hits IS NULL THEN y.hit_array
            WHEN y.hit_array IS NULL THEN ARRAY[t.num_hits]
            ELSE ARRAY[t.num_hits] || y.hit_array
        END AS hit_array,
        CASE 
            WHEN t.unique_users IS NULL THEN y.unique_visitors
            WHEN y.unique_visitors IS NULL THEN ARRAY[t.unique_users]
            ELSE ARRAY[t.unique_users] || y.unique_visitors
        END AS unique_visitors
    FROM 
        today t
    FULL OUTER JOIN yesterday y
        ON t.host = y.host
        AND t.month = y.month
)
INSERT INTO host_activity_reduced (month, host, hit_array, unique_visitors)
SELECT month, host, hit_array, unique_visitors
FROM combined
ON CONFLICT (month, host)
DO UPDATE SET
    hit_array = EXCLUDED.hit_array,
    unique_visitors = EXCLUDED.unique_visitors;

-- View the aggregated records in `host_activity_reduced`
SELECT * FROM host_activity_reduced;
