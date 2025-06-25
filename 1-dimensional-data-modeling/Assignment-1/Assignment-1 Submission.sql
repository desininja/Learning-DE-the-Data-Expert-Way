---------------------------------------------------------------------
-- Task 1: DDL for actors table
---------------------------------------------------------------------


-- An Array of Struct Data Type (custom Data Type)
CREATE TYPE films AS (
		film TEXT,
		votes INTEGER,
		rating REAL,
		filmid TEXT
);

-- An Enum Data Type (custom Data Type)
CREATE TYPE quality_class AS ENUM ('star','good','average','bad');

-- Dropping table actors if it exists
DROP TABLE IF EXISTS ACTORS;



-- Creating Table ACTORS with custom dataType columns
CREATE TABLE ACTORS (
	ACTOR TEXT,
	actorid TEXT,
	FILMS FILMS[],
	QUALITY_CLASS QUALITY_CLASS,
	IS_ACTIVE BOOLEAN,
	FILM_YEAR INTEGER,
	PRIMARY KEY (actorid,film_year)
);
-- PRIMARY KEY: actorid, film_year


---------------------------------------------------------------------
-- Task 2: DDL for actors table
---------------------------------------------------------------------

-- Cumulative table generation query: Below query populates the actors table one year at a time.

INSERT INTO ACTORS
WITH
    -- CTE 1: Select all records from the PREVIOUS year.
    cte_last_year AS (
        SELECT *
        FROM ACTORS
        WHERE film_year = 2018 -- Param_Last_Year
    ),

    -- CTE 2: Aggregate all films for each actor in the CURRENT year.
    cte_current_year_films AS (
        SELECT
            actor,
            actorid,
            year,
            ARRAY_AGG(CAST(ROW(film, votes, rating, filmid) AS films)) AS films
        FROM
            actor_films
        WHERE
            year = 2019 -- Param_Current_Year
        GROUP BY
            actor,
            actorid,
            year
    ),

    -- CTE 3 : Calculate the average rating for the current year's films.
    cte_current_year_avg_rating AS (
        SELECT
            actorid,
            AVG((film_row).rating) as avg_rating
        FROM
            cte_current_year_films,
            UNNEST(films) AS film_row 
        GROUP BY
            actorid
    )

-- Final SELECT: Combine the previous year's data with the current year's data.
SELECT
    COALESCE(cyf.actor, ly.actor) AS actor,
    COALESCE(cyf.actorid, ly.actorid) AS actorid,
    
    -- Combine film arrays: if the actor is new, use current films. If returning, concatenate. If inactive, keep old films.
    CASE
        WHEN ly.films IS NULL THEN cyf.films
        WHEN cyf.year IS NOT NULL THEN ly.films || cyf.films
        ELSE ly.films
    END AS films,
    
    -- Determining the quality_class based on the most recent activity.
    -- If active this year, calculate a new quality_class. If not, carry over last year's quality_class.
    CASE
        WHEN ar.avg_rating > 8 THEN 'star'
        WHEN ar.avg_rating > 7 THEN 'good'
        WHEN ar.avg_rating > 6 THEN 'average'
        WHEN ar.avg_rating IS NOT NULL THEN 'bad' 
        ELSE ly.quality_class -- For inactive actors, carry over their previous class
    END::quality_class AS quality_class,
    
    -- If the actor was active in the current year.
    CASE
        WHEN cyf.year IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS is_active,
    
    -- Set the film year for the record. For inactive actors, this increments the year.
    COALESCE(cyf.year, ly.film_year + 1) AS film_year
FROM
    cte_current_year_films AS cyf
    -- FULL OUTER JOIN is essential to handle all three cases: new, returning, and inactive actors.
    FULL OUTER JOIN cte_last_year AS ly ON cyf.actorid = ly.actorid
    -- LEFT JOIN to the new average rating CTE. This will be NULL for inactive actors.
    LEFT JOIN cte_current_year_avg_rating AS ar ON cyf.actorid = ar.actorid;


---------------------------------------------------------------------
-- Task 3: DDL for actors_history_scd table
---------------------------------------------------------------------

DROP TABLE IF EXISTS actor_history_scd;

------- creating new data type:

create type ac_scd_type AS (
	quality_class quality_class,
	is_active BOOLEAN,
	start_year INTEGER,
	end_year INTEGER
);


-- Creating actor_history_scd table 

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


---------------------------------------------------------------------
-- Task 4: Backfill query for actors_history_scd
---------------------------------------------------------------------

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



-- Checking if data is updated
SELECT * from actor_history_scd;


----------------------------------------------------------------------------------
-- Task 5: Incremental query for actors_history_scd
----------------------------------------------------------------------------------

--Incremental query for actors_history_scd: "incremental" query that combines the 
-- previous year's SCD data with new incoming data from the actors table.

-- CTE 1: Get only the most recent (currently active) records from the previous SCD run.
WITH last_year_scd AS (
SELECT * 
FROM actor_history_scd
WHERE end_year = 2019
AND current_year =2019
),
-- CTE 2: Get only the Historical records from the previous SCD run.
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
-- CTE 3: Get the data for the new year we are processing.
	this_year_data AS (
SELECT * from actors
WHERE film_year =2020
	),
-- CTE 4: To get unchanged records
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
-- CTE 5: To get changed records
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
-- CTE 6: Unnested query to unpack the records.
unnested_changed_records AS (
	SELECT actor,
			actorid,
			(records::ac_scd_type).quality_class,
			(records::ac_scd_type).is_active,
			(records::ac_scd_type).start_year,
			(records::ac_scd_type).end_year

			FROM changed_records
),
-- CTE 7: To Get new records.
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

-- Union all for combining all the cases
SELECT * FROM historical_scd

UNION ALL

SELECT * FROM unchanged_records

UNION ALL

select * from unnested_changed_records

UNION ALL

SELECT * FROM new_records