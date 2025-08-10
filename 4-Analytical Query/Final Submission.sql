/* Q1
A query that does state change tracking for players

A player entering the league should be New
A player leaving the league should be Retired
A player staying in the league should be Continued Playing
A player that comes out of retirement should be Returned from Retirement
A player that stays out of the league should be Stayed Retired
*/

-- Create a table to store the state of each player for each season.
-- This table is designed to be populated iteratively for each season.
CREATE TABLE player_state (
    player_name TEXT,
    height TEXT,
    college TEXT,
    country TEXT,
    current_season INT,
    player_status TEXT,
    PRIMARY KEY (player_name, current_season)
);


-- This query performs an iterative state change tracking for one season (2006).
-- It compares a player's status from the previous season (2005) with their status in the current season (2006).
-- The 'WHERE' clauses for current_season and season would be updated for each year in the iteration.
INSERT INTO player_state (
    player_name,
    height,
    college,
    country,
    current_season,
    player_status
)
WITH
  -- 'yesterday' CTE: Represents the state of all players from the previous season (2005),
  -- by selecting from the already-populated player_state table.
  yesterday AS (
    SELECT
        *
    FROM
      player_state
    WHERE
      current_season = 2005
  ),
  -- 'today' CTE: Represents the state of all players from the current season (2006),
  -- by selecting from the raw player_seasons data.
  today AS (
    SELECT
      player_name,
      height,
      college,
      country,
      season
    FROM
      player_seasons
    WHERE
      season = 2006
  )
SELECT
  -- Use COALESCE to get player and attribute data from either today or yesterday,
  -- preferring the current season's data if available.
  COALESCE(t.player_name, y.player_name) AS player_name,
  COALESCE(t.height, y.height) AS height,
  COALESCE(t.college, y.college) AS college,
  COALESCE(t.country, y.country) AS country,
  -- The current season is derived from either today's season or yesterday's season + 1.
  COALESCE(t.season,y.current_season+1) as current_season, 
  -- The core logic to determine the player's status based on their presence in 'today' and 'yesterday'.
  CASE
    -- 'New': Player exists in the current season but did not exist in the previous one.
    WHEN y.player_status IS NULL
        AND y.player_name IS NULL
        AND t.player_name IS NOT NULL 
        THEN 'New'
    -- 'Returned from Retirement': Player was retired in the previous season but is active again today.
    WHEN y.player_status IN ('Retired','Stayed Retired') 
        AND t.player_name IS NOT NULL 
        THEN 'Returned from Retirement'
    -- 'Continued Playing': Player was active in both the previous and current seasons.
    WHEN y.player_name IS NOT NULL 
        AND t.player_name IS NOT NULL 
        THEN 'Continued Playing'
    -- 'Stayed Retired': Player was retired yesterday and remains retired today.
    WHEN y.player_name IS NOT NULL 
        AND t.player_name IS NULL 
        AND y.player_status  IN ('Retired','Stayed Retired')
        THEN 'Stayed Retired'
     -- 'Retired': Player was active in the previous season but is not active in the current one.
     WHEN y.player_name IS NOT NULL 
        AND t.player_name IS NULL 
        THEN 'Retired'
    -- Catch-all for any other scenario.
    ELSE 'Unknown'
  END AS player_status
FROM today as t
FULL OUTER JOIN yesterday as y ON t.player_name= y.player_name;


/* Q2
A query that uses GROUPING SETS to do efficient aggregations of game_details data

Aggregate this dataset along the following dimensions
player and team
Answer questions like who scored the most points playing for one team?
player and season
Answer questions like who scored the most points in one season?
team
Answer questions like which team has won the most games?
*/


-- Create a table to store the pre-computed aggregations from game and player data.
-- This table will be a compressed and query-friendly version of the raw data.
CREATE TABLE game_aggregates (
    player_name VARCHAR(255),
    team VARCHAR(255),
    season VARCHAR(255),
    total_points INTEGER,
    total_wins INTEGER
);


-- This query uses GROUPING SETS to perform all three aggregations in a single pass over the data.
-- It joins game_details with games to get season and win/loss information.
INSERT INTO game_aggregates
SELECT
    -- COALESCE replaces NULL values from GROUPING SETS with a descriptive string.
    COALESCE(gd.player_name, 'All Players') AS player_name,
    COALESCE(gd.team_abbreviation, 'All Teams') AS team,
    COALESCE(CAST(g.season AS TEXT), 'All Seasons') AS season,
    -- Aggregate total points for each grouping.
    SUM(gd.pts) AS total_points,
    -- Calculate total wins using a conditional sum (1 for a win, 0 for a loss).
    SUM(CASE
            WHEN (gd.team_id = g.home_team_id AND g.home_team_wins = 1)
            OR (gd.team_id = g.visitor_team_id AND g.home_team_wins = 0)
        THEN 1 ELSE 0 END) AS total_wins
FROM
    game_details AS gd
JOIN
    games AS g ON gd.game_id = g.game_id
WHERE
    gd.pts IS NOT NULL
GROUP BY
    -- The GROUPING SETS clause specifies the dimensions for aggregation.
    -- The query will produce a row for each combination listed.
    GROUPING SETS (
        (gd.player_name, gd.team_abbreviation),  -- Aggregates by player and team
        (gd.player_name, g.season),              -- Aggregates by player and season
        (gd.team_abbreviation)                   -- Aggregates by team
    )
ORDER BY
    player_name,
    team,
    season;


-- To answer "Who scored the most points playing for one team?"
-- Query the game_aggregates table for rows where season is the 'All Seasons' summary.
SELECT player_name, team, total_points
FROM game_aggregates
WHERE season = 'All Seasons' AND player_name <> 'All Players'
ORDER BY total_points DESC
LIMIT 1;
-- Answer is "Giannis Antetokounmpo"

-- To answer "Who scored the most points in one season?"
-- Query the game_aggregates table for rows where team is the 'All Teams' summary.
SELECT player_name, season, total_points
FROM game_aggregates
WHERE team = 'All Teams'
ORDER BY total_points DESC
LIMIT 1;
-- Answer is "James Harden"

-- To answer "Which team has won the most games?"
-- Query the game_aggregates table for the overall summary row.
SELECT team, total_wins
FROM game_aggregates
WHERE player_name = 'All Players' AND season = 'All Seasons'
ORDER BY total_wins DESC
LIMIT 1;
-- Answer is "GSW"

/* Q3
A query that uses window functions on game_details to find out the following things:
- What is the most games a team has won in a 90 game stretch?
- How many games in a row did LeBron James score over 10 points a game?
*/

-- This query calculates the maximum number of games a team has won in any 90-game stretch.
WITH team_game_results AS (
    -- Step 1: Join game and game_details tables and determine wins/losses for each team in each game.
    -- This CTE gets a single row per team per game, which is necessary for accurate windowing.
    SELECT
        g.game_date_est,
        gd.team_abbreviation,
        -- The win_indicator is 1 if the team won, 0 if they lost.
        CASE
            WHEN (gd.team_id = g.home_team_id AND g.home_team_wins = 1)
            OR (gd.team_id = g.visitor_team_id AND g.home_team_wins = 0)
            THEN 1
            ELSE 0
        END AS win_indicator
    FROM
        game_details AS gd
    JOIN
        games AS g ON gd.game_id = g.game_id
    GROUP BY
        g.game_date_est,
        gd.team_abbreviation,
        gd.team_id,
        g.home_team_id,
        g.visitor_team_id,
        g.home_team_wins
),
rolling_wins AS (
    -- Step 2: Calculate the rolling sum of wins over a 90-game window for each team.
    SELECT
        team_abbreviation,
        game_date_est,
        SUM(win_indicator) OVER (
            PARTITION BY team_abbreviation
            ORDER BY game_date_est
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) AS wins_in_90_games
    FROM
        team_game_results
)
-- Step 3: Find the maximum number of wins from any 90-game stretch.
SELECT
    MAX(wins_in_90_games) AS most_wins_in_90_game_stretch
FROM
    rolling_wins;

-- ANSWER is "most_wins_in_90_game_stretch"
--                      77

-- This query determines the longest consecutive streak of games where LeBron James scored over 10 points.
WITH lebron_scoring AS (
    -- Step 1: Filter for LeBron James's games and check if he scored over 10 points.
    SELECT
        g.game_date_est,
        gd.pts,
        -- A flag to indicate if he scored over 10 points.
        CASE WHEN gd.pts > 10 THEN 1 ELSE 0 END AS over_10_points_flag
    FROM
        game_details AS gd
    JOIN
        games AS g ON gd.game_id = g.game_id
    WHERE
        gd.player_name = 'LeBron James'
    ORDER BY
        g.game_date_est
),
streak_groups AS (
    -- Step 2: Create a unique group ID for each consecutive streak of > 10 point games.
    -- This is done by subtracting two row numbers, a common "gaps and islands" technique.
    SELECT
        game_date_est,
        over_10_points_flag,
        ROW_NUMBER() OVER (ORDER BY game_date_est) -
        ROW_NUMBER() OVER (PARTITION BY over_10_points_flag ORDER BY game_date_est) AS streak_group
    FROM
        lebron_scoring
),
streak_lengths AS (
    -- Step 3: Count the number of games in each streak group.
    SELECT
        streak_group,
        COUNT(*) AS streak_length
    FROM
        streak_groups
    WHERE
        over_10_points_flag = 1 -- Only consider streaks where he scored > 10 points.
    GROUP BY
        streak_group
)
-- Step 4: Find the maximum streak length.
SELECT
    MAX(streak_length) AS max_consecutive_games_over_10_points
FROM
    streak_lengths;

-- ANSWER is "max_consecutive_games_over_10_points"
--     62