-- Step 1: View Existing Actor-Film Relationships
-- Display all records in the `actor_films` table.
select * from actor_films af;

-- Get the count of distinct actors for each actor ID where count > 1.
select actorid, count(distinct actor) 
from actor_films af  
group by actorid 
having count(actor) > 1;

-- Count the number of occurrences for each actor-film pair.
select actorid, filmid, count(*) 
from actor_films af 
group by 1,2;

-- Step 2: Define Custom Data Types
-- Create a composite type to model films with their details.
create type films as (
    film text,
    votes integer,
    rating real,
    filmid text
);

-- Create an enumerated type for classifying actor quality.
create type quality_class as enum('star', 'good', 'average', 'bad');

-- Step 3: Create `actors` Table
-- Create the `actors` table to store actor details, activity status, films, and quality class.
create table actors (
    actor text,
    actorid text,
    current_year integer,
    is_active boolean,
    films films[], -- Array of film details for each actor.
    quality_class quality_class,
    primary key(actorid, current_year)
);

-- Query to find the range of years in `actor_films`.
select min(year) from actor_films af; -- 1970
select max(year) from actor_films af; -- 2021

-- Step 4: Insert Data into `actors` Table
-- Populate the `actors` table for a specific year (e.g., 1982) based on historical and new data.
insert into actors
with yesterday as (
    select * from actors where current_year = 1981
),
today_raw as (
    select * from actor_films af where af.year = 1982
),
avg_rating as (
    select 
        actorid,
        avg(rating) as rating
    from today_raw
    group by actorid
),
today as (
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
    -- Determine whether the actor is active.
    case
        when t.year is not null then true
        else false
    end as is_active,
    -- Combine films from current and previous years.
    case 
        when y.films is null then t.films -- New actor with no films last year.
        when t.year is not null then y.films || t.films -- Existing actor with new films.
        when t.year is null then y.films -- Existing actor with no new films.
    end as films,
    -- Assign quality class based on the average rating of films.
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

-- View data in the `actors` table.
select * from actors a;

-- Step 5: Create Slowly Changing Dimension (SCD) Type 2 Table
-- Create a history table to track changes in actor attributes over time.
create table actor_history_scd (
    actor text,
    actorid text,
    current_year integer,
    is_active boolean,
    quality_class quality_class,
    start_year integer,
    end_year integer,
    primary key(actorid, start_year)
);

-- Insert records into `actor_history_scd` to implement SCD Type 2 logic.
insert into actor_history_scd
with with_previous as (
    select
        actor,
        actorid,
        current_year,
        is_active,
        quality_class,
        lag(quality_class, 1) over (partition by actorid order by current_year) as previous_quality_class,
        lag(is_active, 1) over (partition by actorid order by current_year) as previous_is_active
    from actors a 	
    where current_year <= 1981
),
with_indicators as (
    select 
        *,
        -- Identify changes in attributes (e.g., activity or quality class).
        case 
            when is_active != previous_is_active or quality_class != previous_quality_class then 1
            else 0
        end as change_indicator	
    from with_previous
),
with_streaks as (
    select
        *,
        sum(change_indicator) over (partition by actorid order by current_year) as streak_id
    from with_indicators
)
select
    actor,
    actorid,
    '1981' as current_year,
    is_active,
    quality_class,
    min(current_year) as start_year,
    max(current_year) as end_year
from with_streaks
group by
    actor,
    actorid,
    is_active,
    quality_class,
    streak_id;

-- View data in `actor_history_scd`.
select * from actor_history_scd;

-- Step 6: Extend SCD for Current Year
-- Drop and recreate an SCD type to represent change events.
drop type scd cascade;
create type scd as (
    is_active boolean,
    quality_class quality_class,
    start_year integer,
    end_year integer
);

-- Merge historical, unchanged, new, and changed records for 1982.
with last_year_scd as (
    select * from actor_history_scd where current_year = 1981 and end_year = 1981
),
historic_scd as (
    select 
        actor,
        actorid,
        is_active,
        quality_class,
        start_year,
        end_year
    from actor_history_scd 
    where end_year < 1981
),
this_year as (
    select 
        actor,
        actorid,
        current_year,
        is_active,
        quality_class
    from actors 
    where current_year = 1982
),
unchanged_records as (
    select
        y.actor,
        y.actorid,
        y.is_active,
        y.quality_class,
        y.start_year,
        y.end_year
    from last_year_scd y
    join this_year t on t.actorid = y.actorid and t.is_active = y.is_active and t.quality_class = y.quality_class
),
new_records as (
    select 	
        t.actor,
        t.actorid,
        t.is_active,
        t.quality_class,
        t.current_year as start_year,
        t.current_year as end_year
    from this_year t
    left join last_year_scd y on y.actorid = t.actorid
    where y.actorid is null
),
changed_records as (
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
        y.is_active != t.is_active or y.quality_class != t.quality_class
),
changed_records_unnested as (
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
select * from new_records;
