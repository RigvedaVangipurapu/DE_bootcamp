--make scd type 2
create table actor_history_scd
(
	actor text,
	actorid text,
	current_year integer,
	is_active boolean,
	quality_class quality_class,
	start_year integer,
	end_year integer,
	primary key(actorid, start_year)
);