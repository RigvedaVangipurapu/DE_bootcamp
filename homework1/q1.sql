create type films as
(
	film text,
	votes integer,
	rating real,
	filmid text
);

create type quality_class as enum('star', 'good', 'average', 'bad');


create table actors (
	actor text,
	actorid text,
	current_year integer,
	is_active boolean,
	films films[],
	quality_class quality_class,
	primary key(actorid, current_year)
);