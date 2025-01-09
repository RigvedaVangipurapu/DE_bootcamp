-- Create a table to store user growth accounting details. 
-- It tracks users' first and last active dates, daily and weekly active states, and active dates history.
create table users_growth_accounting (
	user_id text,
	first_active_date date,
	last_active_date date,
	daily_active_state text,
	weekly_active_state text,
	dates_active date[],
	date date,
	primary key (user_id, date)
);

-- Select sample events for a specific day ('2023-03-01') to check data availability in the events table.
select * 
from events e 
where date_trunc('day', event_time::timestamp) = date('2023-03-01') 
limit 10;

-- Insert data into the `users_growth_accounting` table for a specific day ('2023-01-10'). 
-- This query tracks user activity states by comparing yesterday's (`2023-01-09`) and today's activity.
insert into users_growth_accounting 
with yesterday as (
	select * 
	from users_growth_accounting
	where date = date('2023-01-09')
),
today as (
	select 
		user_id::text,
		date_trunc('day', event_time::timestamp) as today_date,
		count(1)
	from events e 
	where date_trunc('day', event_time::timestamp) = date('2023-01-10')
	and user_id is not null
	group by user_id, date_trunc('day', event_time::timestamp)
)
select 
	coalesce(t.user_id, y.user_id) as user_id,
	coalesce(y.first_active_date, t.today_date) as first_active_date,
	coalesce(t.today_date, y.last_active_date) as last_active_date,
	case 
		when y.user_id is null and t.user_id is not null then 'New'
		when y.last_active_date = t.today_date - interval '1 day' then 'Retained'
		when y.last_active_date < t.today_date - interval '1 day' then 'Resurrected'
		when t.today_date is null and y.last_active_date = y.date then 'Churned'
		else 'Stale'
	end as daily_active_state,
	case 
		when y.user_id is null then 'New'
		when y.last_active_date < t.today_date - interval '7 day' then 'Resurrected'
		when y.last_active_date >= y.date - interval '7 day' then 'Retained'
		when t.today_date is null 
			and y.last_active_date = y.date - interval '7 day' then 'Churned'
		else 'Stale'
	end as weekly_active_state,
	coalesce(y.dates_active, array[]::date[]) 
		|| case 
			when t.user_id is not null then array[t.today_date]
			else array[]::date[]
		   end as date_list,
	coalesce(t.today_date, y.date + interval '1 day') as date
from today t
full outer join yesterday y on y.user_id = t.user_id;

-- Verify the inserted data for a specific day ('2023-01-10') to confirm the update was successful.
select * 
from users_growth_accounting uga 
where uga.date = date('2023-01-10');

-- Aggregate daily active state counts for each date to analyze user activity trends.
select 
	date, 
	daily_active_state, 
	count(1) 
from users_growth_accounting uga 
group by date, daily_active_state;

-- Calculate user retention and engagement metrics by tracking the percentage of active users over time.
select 
	date,
	date - first_active_date as days_since_first_active,
	count(case when daily_active_state in ('Retained', 'Resurrected', 'New') then 1 end) as active_users,
	count(1) as total_users,
	count(case when daily_active_state in ('Retained', 'Resurrected', 'New') then 1 end)::real / count(1) * 100 as pct_active
from users_growth_accounting uga 
-- Uncomment the line below to filter users by a specific first active date.
-- where first_active_date = date('2023-01-01') 
group by date, date - first_active_date 
order by date, date - first_active_date;

-- Count the number of records per user for a specific day ('2023-01-10') to track activity frequency.
select 
	user_id, 
	count(*) as activity_count
from users_growth_accounting uga 
where uga.date = date('2023-01-10') 
group by user_id;
