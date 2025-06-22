# Advanced Data Warehousing: Building Historical Tables with Window Functions

This document provides a comprehensive explanation of an advanced SQL query designed to build a complete historical (or "cumulative") table for NBA player statistics.

Unlike a simple incremental update, this method uses a series of Common Table Expressions (CTEs) and a powerful window function (`array_agg` OVER `...`) to construct the entire history for every player across a range of seasons in a single `INSERT` statement. This approach is powerful, declarative, and idempotent.

---

## 1. Schema Definition

First, we define the necessary data structures for our cumulative table.

### Custom Data Types

```sql
CREATE TYPE season_stats AS (
    season INTEGER,
    gp     INTEGER,
    pts    REAL,
    reb    REAL,
    ast    REAL
);

CREATE TYPE scoring_class AS ENUM ('star', 'good', 'average', 'bad');
```
-   **`season_stats`**: A custom composite type to bundle a player's statistics for a single season. This is the correct version that matches the `ROW` constructor in the query.
-   **`scoring_class`**: An enumerated type to ensure data integrity for classifying player performance.

### The `players` Table Schema

```sql
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
    years_since_last_active INTEGER,
    current_season INTEGER,
    is_active BOOLEAN,
    PRIMARY KEY (player_name, current_season)
);
```
-   This is the destination table. The key columns are `season_stats[]` (an array to hold the history), `current_season` (to partition the data by year), and the composite `PRIMARY KEY` which makes the table idempotent.

---

## 2. The Historical Load Query Explained

The core of this process is a single `INSERT ... SELECT` statement that uses multiple CTEs to prepare the data.

```sql
insert into players
-- The query is wrapped in CTEs to break the logic into understandable steps.
with
-- CTE 1: Create a complete list of all seasons to be processed.
years as (
	select generate_series(1996, 2022) as season
),

-- CTE 2: Find the debut season for every player.
p as (
	select player_name , MIN(season) as first_season
	from player_seasons
	group by player_name
),

-- CTE 3: Create a "dense grid". This is a crucial step.
-- It generates a row for every player for every single season from their debut until 2022.
-- This ensures we process players even in years they were inactive.
players_and_seasons as (
	select *
	from p
	join years y on p.first_season <= y.season
),

-- CTE 4: The main engine. This uses a window function to build the historical stats array.
windowed as (
	select
        ps.player_name,
        ps.season,
        -- This is the window function call:
        array_remove(
            array_agg(
                -- For each row, create a season_stats object only if the player was active that year.
                case when p1.season is not null then
                    cast(row(p1.season, p1.gp, p1.pts, p1.reb, p1.ast) as season_stats)
                end
            -- The OVER clause defines the "window" for the aggregation.
            ) over (partition by ps.player_name order by ps.season),
            null -- Clean up the array by removing nulls from inactive years.
        ) as seasons
	from players_and_seasons ps
	-- LEFT JOIN to the source data. If a player was inactive in a given year, p1.* will be NULL.
	left join player_seasons p1 on ps.player_name = p1.player_name and ps.season = p1.season
	order by ps.player_name, ps.season
),

-- CTE 5: Aggregate static player metadata that doesn't change.
static as (
	select player_name,
        max(height) as height,
        max(college) as college,
        max(country) as country,
        max(draft_year) as draft_year,
        max(draft_round) as draft_round,
        max(draft_number) as draft_number
	from player_seasons ps
	group by player_name
)

-- Final SELECT: Join the prepared CTEs and calculate the final fields.
select
	w.player_name,
	s.height,
	s.college,
	s.country,
	s.draft_year,
	s.draft_number,
	s.draft_round,
	w.seasons as season_stats,
	-- Classify the player based on their last active season's points.
	case
        when (w.seasons[cardinality(w.seasons)]).pts > 20 then 'star'
        when (w.seasons[cardinality(w.seasons)]).pts > 15 then 'good'
        when (w.seasons[cardinality(w.seasons)]).pts > 10 then 'average'
        else 'bad'
	end :: scoring_class as scoring_class, -- Corrected typo
	-- Calculate years since last activity.
	w.season - (w.seasons[cardinality(w.seasons)]).season as years_since_last_active,
	w.season as current_season,
	-- A boolean flag to easily identify if the player was active in this specific season.
	(w.seasons[cardinality(w.seasons)]).season = w.season as is_active
from windowed w
join static s on w.player_name = s.player_name;
```

### Deeper Dive into the `windowed` CTE

This is the most complex and important part of the query.

-   **`array_agg(...) OVER (PARTITION BY ... ORDER BY ...)`**: This is the window function.
    -   **`PARTITION BY ps.player_name`**: This tells the function to operate independently for each player. It resets the aggregation for every new player.
    -   **`ORDER BY ps.season`**: This is critical. It tells the function to process the rows for each player in chronological order. When `array_agg` runs, its window frame, by default, includes all rows from the start of the partition up to the *current row*.
    -   **How it works**: For a given player in the year 2000, `array_agg` will look at all their rows from their debut up to 2000, collect the non-null `season_stats` objects into an array, and assign that array to the row for the year 2000. For the year 2001, it does the same thing, now including the 2001 data, resulting in a slightly larger array. This is how the historical array is built up year by year.
-   **`array_remove(..., null)`**: The `CASE` statement inside `array_agg` produces `NULL` for years a player was inactive. This function efficiently cleans those `NULL`s out of the final array.

This single query effectively builds a complete, versioned history for every player in your dataset, providing a powerful foundation for time-series analysis.
