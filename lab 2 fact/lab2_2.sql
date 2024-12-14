-- Generate a series of dates from January 1, 2023, to January 31, 2023
select * from generate_series(date('2023-01-01'), date('2023-01-31'), interval '1 day');

-- Get a list of all dates where the user was active
with users as (
    -- Fetch user activity data specifically for January 31, 2023
    select * from users_cumulated
    where date = date('2023-01-31')
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
            when dates_active @> array[date(series_date)] 
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
    (sum(placeholder_int_value)::bigint)::bit(32) as monthly_activity_bits, -- Consolidate all daily activity into a single 32-bit binary value
    bit_count((sum(placeholder_int_value)::bigint)::bit(32)) as dim_is_monthly_active, -- Count how many days in the month the user was active
    bit_count((CAST('11111110000000000000000000000000' AS bit(32)) & (SUM(placeholder_int_value)::bigint)::bit(32))) as dim_is_weekly_active, -- Count how many weeks the user was active
    bit_count((CAST('10000000000000000000000000000000' AS bit(32)) & (SUM(placeholder_int_value)::bigint)::bit(32))) as dim_is_daily_active -- Count how many days the user was active on a specific day
from placeholderints
group by user_id;
