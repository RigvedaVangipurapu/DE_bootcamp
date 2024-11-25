insert into actors
with yesterday as (
	select * from actors where current_year = 1981
),
today_raw as 
(
	select * from actor_films af where af.year = 1982
),
avg_rating as 
(
	select 
		actorid,
		avg(rating) as rating
	from today_raw
	group by actorid
),
today as
(
	   select 
        	actorid,
        	actor,
        	year,
        	array_agg(row(
        		film, 
        		votes, 
        		rating, 
        		filmid
        	)::films ) as films
    from today_raw
    group by actorid, actor, year
)
select 
	coalesce (t.actor, y.actor) as actor,
	coalesce (t.actorid, y.actorid) as actorid,
	coalesce(t.year, y.current_year + 1) as current_year,
	--is_active
	case
		when t.year is not null then 1
		else 0
	end::boolean as is_active,
	-- films
	case 
		when y.films is null then t.films -- new actor, no films last year 
		when t.year is not null then --he had movies yesterday, he has movies today as well
			y.films || t.films
		when t.year is null then --he had movies yesterday but none today
			y.films
	end as films,
	--quality class
	case
		when t.year is not null then 
			case 
				when avrg.rating > 8 then 'star'
				when avrg.rating > 7 then 'good'
				when avrg.rating > 6 then 'average'
				else 'bad'
			end::quality_class
		else
			y.quality_class
	end::quality_class as quality_class
from today t
left join avg_rating avrg on t.actorid = avrg.actorid
full outer join yesterday y on y.actorid = t.actorid;

select * from actors a ;
	