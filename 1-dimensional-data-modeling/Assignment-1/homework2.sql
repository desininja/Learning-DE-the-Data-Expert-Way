------- creating new data type:

create type ac_scd_type AS (
	quality_class quality_class,
	is_active BOOLEAN,
	start_year INTEGER,
	end_year INTEGER
);


---------------- actor_history_scd -------------
DROP TABLE actor_history_scd;

CREATE TABLE actor_history_scd (
	actor TEXT,
	actorid TEXT,
	quality_class quality_class,
	is_active BOOLEAN,
	start_year INTEGER,
	end_year INTEGER,
	current_year INTEGER,
	PRIMARY KEY(actorid,start_year)
)



-- "backfill" query that can populate the entire actors_history_scd table in a single query.
INSERT INTO actor_history_scd
WITH with_previous AS (
	SELECT actor,
		actorid,
		quality_class,
		lag(quality_class,1) over(partition by actorid order by film_year) as previous_quality_class,
		is_active,
		lag(is_active,1) over(partition by actorid order by film_year) as previous_is_active,
		film_year
FROM actors
WHERE film_year <=2019
),

with_indicators AS (
SELECT *,
		CASE
			WHEN quality_class <> previous_quality_class THEN 1
			WHEN is_active <> previous_is_active THEN 1
			ELSE 0
		END AS change_indicator
FROM with_previous
),

with_streak AS (

SELECT *,
		sum(change_indicator)
			OVER(partition by actorid order by film_year) as streak_identifier
FROM with_indicators

)

SELECT actor,
		actorid,
		quality_class,
		is_active,
		min(film_year) as start_year,
		max(film_year) as end_year,
		2019 as current_year


FROM with_streak
GROUP BY actor,actorid,streak_identifier,is_active,quality_class
ORDER BY actor,streak_identifier




SELECT * from actor_history_scd;



--
----------------------------------------------------------------------------------
--Incremental query for actors_history_scd: Write an "incremental" query that combines the 
-- previous year's SCD data with new incoming data from the actors table.

WITH last_year_scd AS (
SELECT * 
FROM actor_history_scd
WHERE end_year = 2019
AND current_year =2019
),
	historical_scd AS (
SELECT actor,
		actorid,
		quality_class,
		is_active,
		start_year,
		end_year
FROM actor_history_scd
WHERE end_year <2019
AND current_year=2019
	),

	this_year_data AS (
SELECT * from actors
WHERE film_year =2020
	),

	unchanged_records AS (
SELECT ts.actor,
		ts.actorid,
		ts.quality_class,
		ts.is_active,
		ls.start_year,
		ts.film_year as end_year
FROM this_year_data ts
JOIN last_year_scd ls
ON ls.actorid = ts.actorid
WHERE ts.quality_class = ls.quality_class and ts.is_active = ls.is_active
),

changed_records AS (

SELECT ts.actor,
		ts.actorid,
		UNNEST(
		ARRAY[
			ROW(ls.quality_class,
				ls.is_active,
				ls.start_year,
				ls.end_year)::ac_scd_type,
			ROW(ts.quality_class,
				ts.is_active,
				ts.film_year,
				ts.film_year)::ac_scd_type
		]
		) as records
FROM this_year_data ts
LEFT JOIN last_year_scd ls
ON ls.actorid = ts.actorid
WHERE (ts.quality_class <> ls.quality_class OR ts.is_active <> ls.is_active)
		
),

unnested_changed_records AS (
	SELECT actor,
			actorid,
			(records::ac_scd_type).quality_class,
			(records::ac_scd_type).is_active,
			(records::ac_scd_type).start_year,
			(records::ac_scd_type).end_year

			FROM changed_records
),

new_records AS (
	SELECT ts.actor,
			ts.actorid,
			ts.quality_class,
			ts.is_active,
			ts.film_year as start_year,
			ts.film_year as end_year

	FROM this_year_data ts
	LEFT JOIN last_year_scd ls
	on ts.actorid = ls.actorid
	where ls.actorid is null
)


SELECT * FROM historical_scd

UNION ALL

SELECT * FROM unchanged_records

UNION ALL

select * from unnested_changed_records

UNION ALL

SELECT * FROM new_records