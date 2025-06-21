-- Analyzing the table that we are going to work on.
SELECT * FROM player_seasons;


-- Creating Struct type 
CREATE TYPE season_stats AS(
    season INTEGER,
    gp      INTEGER,
    pts     INTEGER,
    reb     REAL,
    ast     REAL
)

-- Creating an ENUM Type

CREATE TYPE scoring_class as ENUM ('star','good','average','bad');

-- Creating a cumulative table

CREATE TABLE players (
	player_name TEXT,
	height TEXT,
	college TEXT,
	country TEXT,
	draft_year TEXT,
	draft_round TEXT,
	draft_number TEXT,
	season_stats season_stats[],
	scoring_class scoring_class,
	years_since_last_season INTEGER,
	current_season INTEGER,
	PRIMARY KEY (player_name, current_season)
)



-- Query to update the cumulative table.
INSERT INTO players
WITH yesterday AS ( --this will work as historical table 
	SELECT * FROM players
	WHERE current_season =2000
),

	today AS( -- this will work as new table, where live incoming data is present
			SELECT * FROM player_seasons
			WHERE season =2001
	)

	SELECT 
			COALESCE(t.player_name, y.player_name) AS player_name,
			COALESCE(t.height, y.height) as height,
			COALESCE(t.college, y.college) as college,
			COALESCE(t.country, y.country) as country,
			COALESCE(t.draft_year, y.draft_year) as draft_year,
			COALESCE(t.draft_round, y.draft_round) as draft_round,
			COALESCE(t.draft_number, y.draft_number) as draft_number,
			CASE WHEN y.season_stats IS NULL 
				 THEN ARRAY[ROW(
								t.season,
								t.gp,
								t.reb,
								t.ast
				 )::season_stats]
			WHEN t.season IS NOT NULL THEN y.season_stats || ARRAY[ROW(
											 t.season,
											 t.gp,
											 t.reb,
											 t.ast)::season_stats]
			ELSE y.season_stats
			END as season_stats,
			CASE
				WHEN t.season IS NOT NULL THEN 
					CASE WHEN t.pts >20 THEN 'star'
						 WHEN t.pts >15 THEN 'good'
						 WHEN t.pts > 10 THEN 'average'
						 ELSE 'bad'
					END::scoring_class
				ELSE y.scoring_class
			END as scoring_class,

			CASE
				WHEN t.season IS NOT NULL THEN 0
				ELSE y.years_since_last_season +1
			END as years_since_last_season,
			COALESCE(t.season,y.current_season +1) as current_season

		FROM today AS t FULL OUTER JOIN yesterday AS y
			ON t.player_name = y.player_name;



-- Analytical queries on Cumulative table i.e., players
select * from players where current_season =2001

SELECT * from players WHERE current_season=2000
and player_name = 'Michael Jordan';


WITH unnested AS(
SELECT player_name,
	   unnest(season_stats)::season_stats as season_stats
	   from players
WHERE player_name = 'Michael Jordan'
AND current_season =2001 )

SELECT player_name,
	   (season_stats::season_stats).*
FROM unnested;




WITH unnested AS(
SELECT player_name,
	   unnest(season_stats)::season_stats as season_stats
	   from players
WHERE  current_season =2001 )

SELECT player_name,
	   (season_stats::season_stats).*
FROM unnested



DROP TABLE players;


SELECT * from player_seasons WHERE  player_name = 'Michael Jordan';

SELECT 
		player_name,
		(season_stats[CARDINALITY(season_stats)]::season_stats).pts/
		CASE WHEN (season_stats[1]::season_stats).pts = 0 THEN 1 ELSE (season_stats[1]::season_stats).pts END
		
FROM players
WHERE current_season =2001
AND scoring_class = 'star'
order by 2 DESC;