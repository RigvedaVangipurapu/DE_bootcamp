create type scd as(
	is_active boolean,
	quality_class quality_class,
	start_year integer,
	end_year integer
);

with last_year_scd as (
	select * from actor_history_scd where current_year = 1981 and end_year = 1981
)
, historic_scd as (
	select 
		actor,
		actorid,
		is_active,
		quality_class,
		start_year,
		end_year
	from actor_history_scd 
	where end_year < 1981
)
, this_year as (
	select 
		actor,
		actorid,
		current_year,
		is_active,
		quality_class
	from actors 
	where current_year = 1982
)
, unchanged_records as (
	select
		y.actor,
		y.actorid,
		y.is_active,
		y.quality_class,
		y.start_year,
		y.end_year
	from last_year_scd y
	join this_year t on t.actorid = y.actorid and t.is_active = y.is_active and t.quality_class = y.quality_class
)
,  new_records as (
	select 	
		t.actor,
		t.actorid,
		t.is_active,
		t.quality_class,
		t.current_year as start_year,
		t.current_year as end_year
	from this_year t
	left join last_year_scd y on y.actorid = t.actorid
	where y.actorid is null --117
)
, changed_records as (
	select 
		t.actor,
		t.actorid,
		unnest(array[
		row(t.is_active, t.quality_class, t.current_year, t.current_year)::scd,
		row(y.is_active, y.quality_class, y.start_year, y.end_year)::scd
		]) as records
	from this_year t
	join last_year_scd y on y.actorid = t.actorid
	where 
		y.is_active != t.is_active or y.quality_class != t.quality_class --2002
)
, changed_records_unnested as (
	select 
		actor,
		actorid,
		(records).is_active,
		(records).quality_class,
		(records).start_year,
		(records).end_year
	from changed_records 
)
select * from historic_scd
union all
select * from unchanged_records
union all
select * from changed_records_unnested
union all
select * from new_records; --10383
