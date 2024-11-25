insert into actor_history_scd
with with_previous as
(
	select
		actor,
		actorid,
		current_year,
		is_active,
		quality_class,
		lag(quality_class,1) over (partition by actorid order by current_year) as previous_quality_class,
		lag(is_active,1) over (partition by actorid order by current_year) as previous_is_active
	from actors a 	
	where current_year <= 1981
)
, with_indicators as (
	select 
		*,
		case 
				when is_active != previous_is_active or quality_class != previous_quality_class then 1
				else 0
		end as change_indicator	
	from with_previous
)
, with_streaks as (
	select
	*,
	sum(change_indicator) over (partition by actorid order by current_year) as streak_id
	from with_indicators
)
select
	actor ,
	actorid ,
	'1981' as current_year ,
	is_active ,
	quality_class ,
	min(current_year) as start_year,
	max(current_year) as end_year
from with_streaks
group by
	actor,
	actorid,
	is_active,
	quality_class,
	streak_id;

select * from actor_history_scd;
