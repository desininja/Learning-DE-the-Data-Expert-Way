DROP TABLE players_scd;
CREATE TABLE players_scd (
		player_name TEXT,
		scoring_class scoring_class,
		is_active BOOLEAN,
		start_season INTEGER,
		end_season INTEGER,
		current_season INTEGER,
		PRIMARY KEY(player_name,start_season)

);
INSERT INTO players_scd
WITH with_previous AS (

SELECT 
		player_name, 
		current_season,
		scoring_class,
		is_active,
		LAG(scoring_class,1) OVER(PARTITION BY player_name ORDER BY current_season) as previous_scoring_class,
		LAG(is_active,1) OVER(PARTITION BY player_name ORDER BY current_season) as previous_is_active
		
FROM players
WHERE current_season <=2021),

with_indicators AS(

SELECT *,
		CASE 
			WHEN scoring_class <> previous_scoring_class THEN 1
			WHEN is_active <> previous_is_active THEN 1
			ELSE 0
		END AS change_indicator
FROM with_previous),

with_streak AS (


SELECT *,
		SUM(change_indicator)
			OVER(PARTITION BY player_name ORDER BY current_season) as streak_identifier
FROM with_indicators)

SELECT player_name, 
       scoring_class,
	   is_active,
		min(current_season) as start_season,
		max(current_season) as end_season,
		2021 as current_season

FROM with_streak
GROUP BY player_name, streak_identifier, is_active, scoring_class
ORDER BY player_name,streak_identifier
;

select * from players_scd;







-------------------------------------------------------------------------


WITH last_season_scd AS (
SELECT * FROM players_scd
WHERE current_season=2021
and end_season =2021
),
	historical_scd AS (
SELECT player_name,
	   scoring_class,
	   is_active,
	   start_season,
	   end_season
	    FROM players_scd
WHERE current_season =2021
AND end_season <2021
	),
	this_season_data AS (
SELECT * FROM players
WHERE current_season =2022
	),

	unchanged_records AS (

SELECT ts.player_name,
		ts.scoring_class,
		ts.is_active,
		ls.start_season,
		ts.current_season as end_season
FROM this_season_data ts
	JOIN last_season_scd ls 
	ON ls.player_name = ts.player_name
	WHERE ts.scoring_class = ls.scoring_class and ts.is_active = ls.is_active
	),
	changed_records AS (

SELECT ts.player_name,
		UNNEST(
		ARRAY[
			ROW(ls.scoring_class,
				ls.is_active,
				ls.start_season,
				ls.end_season
			)::scd_type,
			ROW(ts.scoring_class,
				ts.is_active,
				ts.current_season,
				ts.current_season
			)::scd_type
		]) as records
FROM this_season_data ts
LEFT JOIN last_season_scd ls 
	ON ls.player_name = ts.player_name
	WHERE (ts.scoring_class <> ls.scoring_class OR ts.is_active <> ls.is_active) 
	),

	unnested_changed_records AS (
		select player_name,
		(records::scd_type).scoring_class,
		(records::scd_type).is_active,
		(records::scd_type).start_season,
		(records::scd_type).end_season

		FROM changed_records

	),

	new_records AS (
		SELECT ts.player_name,
				ts.scoring_class,
				ts.is_active,
				ts.current_season as start_season,
				ts.current_season as end_season

		FROM this_season_data ts
		LEFT JOIN last_season_scd ls
		ON ts.player_name = ls.player_name

		WHERE ls.player_name is null
	)


SELECT * FROM historical_scd

UNION ALL

select * from unnested_changed_records

UNION ALL

SELECT * FROM new_records