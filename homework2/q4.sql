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

with users as (
    -- Fetch user activity data specifically for January 31, 2023
    select * from user_devices_cumulated udc 
    where date = date('2023-01-01')
),
series as (
    -- Generate a series of dates for the entire month of January 2023
    select * from generate_series(date('2023-01-01'), date('2023-01-31'), interval '1 day') as series_date
),
-- Using powers of 2. If the user was active on a given date, the difference between that date and the series date is zero. 
-- This resolves to 2^32, which results in a binary value of 1 followed by 31 zeros.
-- For example, if a user was active 27 days ago, the `pow` function results in 16, which is represented as 000000000000000000000000001000.
-- Essentially, each day is represented as a position in the binary. Summing up these binary values gives the user's activity history.
placeholderints as (
    select 
        case 
            when device_activity_datelist @> array[date(series_date)] 
                then pow(2, 32 - (date - date(series_date)))::bigint -- Calculate the power of 2 based on the date difference
                else 0 -- Assign 0 if the user was not active on that date
        end as placeholder_int_value,
        * -- Include all other columns from the `users` and `series` tables
    from users u
    cross join series s
    -- Uncomment the line below to filter for a specific user by user_id
    -- where user_id = '439578290726747300'
)
select 
    user_id,
    (sum(placeholder_int_value)::bigint)::bit(32) as datelist_int, -- Consolidate all daily activity into a single 32-bit binary value
    bit_count((sum(placeholder_int_value)::bigint)::bit(32)) as dim_is_monthly_active -- Count how many days in the month the user was active
from placeholderints
group by user_id;