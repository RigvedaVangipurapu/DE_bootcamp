-- View all events in the events table
select * from events;

-- Drop the `users_cumulated` table if it already exists
drop table users_cumulated;

-- Create a new table `users_cumulated` to track user activity
create table users_cumulated
(
    user_id text, -- Unique identifier for each user
    dates_active date[], -- List of dates where the user was active
    date date, -- Current date being processed
    primary key (user_id, date) -- Composite primary key to ensure uniqueness for each user and date combination
);

-- Trying to build a table from January 1, 2023, to January 31, 2023. Start with the "yesterday" date 
-- (2022-12-31 or the last date already processed) and incrementally build up to the target date.

insert into users_cumulated
with yesterday as (
    -- Select all users and their activity up to the date being processed (e.g., January 30, 2023)
    select
        *
    from users_cumulated
    where date = date(cast('2023-01-30' as timestamp)) -- Process data for the last day available
    -- start with 2022-12-31 and make your way up
    -- where date = date(cast('2022-12-31' as timestamp))
),
today as (
    -- Fetch activity data for users on the current date (e.g., January 31, 2023)
    select 
        cast(user_id as text) as user_id, -- Convert user_id to text
        date(cast(event_time as timestamp)) as date_active -- Extract the date part of the event timestamp
    from events e 
        -- start with 2023-01-01 and make your way up
    where date(cast(event_time as timestamp)) = date('2023-01-31') -- Filter events that occurred on January 31, 2023
      and user_id is not null -- Exclude records with null user IDs
    group by user_id, date(cast(event_time as timestamp)) -- Group by user ID and active date to deduplicate records
)
select 
    coalesce(t.user_id, y.user_id) as user_id, -- Use user_id from `today` if available; otherwise, fall back to `yesterday`
    case 
        -- If the user had no prior activity (`y.dates_active` is null), initialize with today's active date
        when y.dates_active is null then array[t.date_active]
        -- If there is no activity today (`t.date_active` is null), retain prior active dates
        when t.date_active is null then y.dates_active
        -- Otherwise, combine today's active date with prior active dates
        else array[t.date_active] || y.dates_active
    end as dates_active,
    coalesce(t.date_active, y.date + interval '1 day') as date -- Use today's date if available; otherwise, increment yesterday's date
from today t
full outer join yesterday y on y.user_id = t.user_id; -- Combine records from both `today` and `yesterday` to ensure no user is missed
