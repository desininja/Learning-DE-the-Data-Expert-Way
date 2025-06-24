SELECT * FROM actor_films;

CREATE TYPE films AS (
		film TEXT,
		votes INTEGER,
		rating REAL,
		filmid TEXT
);

CREATE TYPE quality_class AS ENUM ('star','good','average','bad');



CREATE TABLE ACTORS (
	FILMS FILMS[],
	QUALITY_CLASS QUALITY_CLASS,
	IS_ACTIVE BOOLEAN
);









INSERT INTO ACTORS
WITH
    -- CTE 1: Select all records from the PREVIOUS year (e.g., 1971).
    cte_last_year AS (
        SELECT *
        FROM ACTORS
        WHERE film_year = 2018 -- Param_Last_Year
    ),

    -- CTE 2: Aggregate all films for each actor in the CURRENT year (e.g., 1972).
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

    -- CTE 3 (NEW): Efficiently calculate the average rating for the current year's films.
    -- This avoids repeating the calculation multiple times in the final SELECT.
    cte_current_year_avg_rating AS (
        SELECT
            actorid,
            AVG((film_row).rating) as avg_rating
        FROM
            cte_current_year_films,
            UNNEST(films) AS film_row -- Unpack the array to access the rating field
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
    
    -- CORRECTED LOGIC: Determine quality_class based on the most recent activity.
    -- If active this year, calculate a new class. If not, carry over last year's class.
    CASE
        WHEN ar.avg_rating > 8 THEN 'star'
        WHEN ar.avg_rating > 7 THEN 'good'
        WHEN ar.avg_rating > 6 THEN 'average'
        WHEN ar.avg_rating IS NOT NULL THEN 'bad' -- Catches avg_rating <= 6
        ELSE ly.quality_class -- For inactive actors, carry over their previous class
    END::quality_class AS quality_class,
    
    -- Determine if the actor was active in the current year.
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


SELECT * FROM ACTORS
where actor = 'Mike Henry';
SELECT actor, count(*) FROM ACTORS
group by 1 order by 2 DESC
SELECT * FROM actor_films;


-- -- An Array of Struct Data Type
-- CREATE TYPE films AS (
-- 		film TEXT,
-- 		votes INTEGER,
-- 		rating REAL,
-- 		filmid TEXT
-- );

-- -- An Enum Data Type
-- CREATE TYPE quality_class AS ENUM ('star','good','average','bad');

-- DROP TABLE ACTORS;
-- -- Table with custom dataType columns
-- CREATE TABLE ACTORS (
-- 	ACTOR TEXT,
-- 	actorid TEXT,
-- 	FILMS FILMS[],
-- 	QUALITY_CLASS QUALITY_CLASS,
-- 	IS_ACTIVE BOOLEAN,
-- 	FILM_YEAR INTEGER,
-- 	PRIMARY KEY (actorid,film_year)
-- );
-- -- PRIMARY KEY: actorid, filmid, film_year

-- -- Cumulative table generation query: Write a query that populates the actors table one year at a time.

-- -- min 1970 and Max 2021
-- select min(year),max(year) from actor_films;


-- INSERT INTO ACTORS
-- WITH cte_last_year AS (
-- 		SELECT * FROM actors
-- 		WHERE film_year = 1971
-- 		),
-- 	cte_current_year AS (
-- 		SELECT
-- 			actor,
--     		actorid,
-- 			year,
--     		ARRAY_AGG(CAST(ROW(film, votes, rating, filmid) AS films)) AS films
-- 		FROM
--     		actor_films
-- 		WHERE YEAR =1972
-- 		GROUP BY
-- 			actor,
--     		actorid,
-- 			year
-- 		)

-- SELECT 
-- 		COALESCE(cy.actor,ly.actor) AS actor,
-- 		COALESCE(cy.actorid,ly.actorid) as actorid,
-- 		CASE WHEN ly.films IS NULL THEN cy.films
-- 			 WHEN cy.year IS NOT NULL THEN ly.films || cy.films
-- 			 ELSE ly.films
-- 			 END as films,
-- 		CASE WHEN ( SELECT
--             			AVG((film_row).rating)
--         			FROM
--             			UNNEST(cy.films) AS film_row
--     				)> 8 THEN 'star'
-- 			 WHEN ( SELECT
--             			AVG((film_row).rating)
--         			FROM
--             			UNNEST(cy.films) AS film_row
--     				)> 7 THEN 'good'
-- 			WHEN ( SELECT
--             			AVG((film_row).rating)
--         			FROM
--             			UNNEST(cy.films) AS film_row
--     				)> 6 THEN 'average'
-- 			ELSE 'bad' END :: quality_class as quality_class,
-- 		CASE WHEN cy.year is not null then true 
-- 		ELSE false END as is_active,
-- 		COALESCE(cy.year,ly.film_year +1) as film_year
		
-- FROM cte_current_year as cy 
-- 	FULL OUTER JOIN cte_last_year as ly
-- 	ON cy.actorid = ly.actorid



-- select * from actors
-- where actor = 'Brigitte Bardot';




-- -----


-- SELECT
--     actor,
-- 	actorid,
--     ARRAY_REMOVE(ARRAY_AGG (
--         cast(row(film,votes,rating,filmid) as films))
--         OVER(partition by actor,actorid,year order by year),null) as films
-- FROM
--     actor_films
-- where year=1970 and actor ='Christopher Lee'

-- ------------

-- select actor,count(*) from actor_Films
-- where year =1970
-- group by actor
-- order by 2 DESC;

-- select * from actor_Films
-- where year =1970 and actor ='Christopher Lee';


-- SELECT
--     actor,
--     actorid,
--     ARRAY_AGG(
--         CAST(ROW(film, votes, rating, filmid) AS films) ORDER BY film
--     ) AS films,
-- 	year
-- FROM
--     actor_films
-- WHERE
--     year = 1970 
-- GROUP BY
--     actor, actorid,year;






-- WITH cte_current_year AS (
-- 	SELECT * --,actorid || filmid || year as record_id
-- 	FROM actor_films
-- 	WHERE year =1970
-- 	)
	

-- SELECT
--     actorid,
--     ARRAY_AGG(
--         CAST(ROW(film, votes, rating, filmid) AS films) ORDER BY film
--     ) AS films
-- FROM
--     cte_current_year
-- GROUP BY
--     actorid