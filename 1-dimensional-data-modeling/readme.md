# NBA Player Stats: Cumulative Table Analysis

This document explains a series of SQL queries designed to build and maintain a **cumulative table** for NBA player statistics. A cumulative table is a powerful data warehousing concept where historical data is preserved and aggregated over time. Instead of overwriting old records, we append new information, allowing for rich historical analysis.

The example uses a fictional dataset of player seasonal stats to demonstrate how to:
1.  Define a robust schema with custom data types.
2.  Incrementally update the cumulative table with new seasonal data.
3.  Run complex analytical queries on the aggregated historical data.

---

## 1. Schema Definition and Setup

Before we can load data, we need to define the structure of our database tables and the types of data they will hold.

### `CREATE TYPE season_stats`
```sql
CREATE TYPE season_stats AS(
    season INTEGER,
    gp     INTEGER,
    pts    INTEGER,
    reb    REAL,
    ast    REAL
);
```
**Explanation:**
-   **`CREATE TYPE`**: This command defines a new, custom composite data type.
-   **`season_stats`**: This is the name of our custom type. Think of it as creating a "struct" or a mini-template for a group of related data.
-   **Fields (`season`, `gp`, etc.)**: Instead of having many separate columns in our main table for each season's stats, we can bundle them together into a single `season_stats` object. This is especially useful when we want to store an array or list of these objects for a player's entire career.

### `CREATE TYPE scoring_class`
```sql
CREATE TYPE scoring_class AS ENUM ('star', 'good', 'average', 'bad');
```
**Explanation:**
-   **`ENUM` (Enumerated Type)**: This creates a special data type that can only hold one of a predefined list of string values.
-   **Benefit**: Using an `ENUM` ensures data integrity. The `scoring_class` column can *only* contain 'star', 'good', 'average', or 'bad', preventing typos or invalid entries. It's also more storage-efficient than a standard `TEXT` field.

### `CREATE TABLE players`
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
    years_since_last_season INTEGER,
    current_season INTEGER,
    PRIMARY KEY (player_name, current_season)
);
```
**Explanation:**
This is our main cumulative table. Here’s a breakdown of its key columns:
-   **`season_stats season_stats[]`**: This is the core of our cumulative design. It's an **array** of our custom `season_stats` type. For each player, this single column will hold their entire season-by-season statistical history.
-   **`years_since_last_season INTEGER`**: A counter to track player activity. If a player has new data for the current season, this will be `0`. If they were inactive, it will be incremented.
-   **`current_season INTEGER`**: Acts as a partition key or version tracker. It tells us the most recent season this row represents.
-   **`PRIMARY KEY (player_name, current_season)`**: This composite key ensures that for any given season, there is only one record per player. It's essential for managing the historical snapshots correctly.

---

## 2. The Cumulative Update Query

This is the most critical query. It takes the existing data from our `players` table (the history up to *last year*) and merges it with new data from `player_seasons` (the data for *this year*).

```sql
INSERT INTO players
-- Use Common Table Expressions (CTEs) to define our data sources
WITH yesterday AS (
    SELECT * FROM players
    WHERE current_season = 2000 -- State of our cumulative table at year N-1
),
today AS (
    SELECT * FROM player_seasons
    WHERE season = 2001 -- New incoming data for year N
)
-- The main SELECT statement to merge the data
SELECT
    -- Merge player metadata, preferring new data from 'today'
    COALESCE(t.player_name, y.player_name) AS player_name,
    COALESCE(t.height, y.height) as height,
    COALESCE(t.college, y.college) as college,
    -- ... (other COALESCE for metadata) ...

    -- Logic to update the historical stats array
    CASE
        WHEN y.season_stats IS NULL THEN -- Player is new this season
            ARRAY[ROW(t.season, t.gp, t.pts, t.reb, t.ast)::season_stats]
        WHEN t.season IS NOT NULL THEN -- Player is returning
            y.season_stats || ARRAY[ROW(t.season, t.gp, t.pts, t.reb, t.ast)::season_stats]
        ELSE -- Player was inactive this season
            y.season_stats
    END as season_stats,

    -- Logic to update the player's scoring classification
    CASE
        WHEN t.season IS NOT NULL THEN
            CASE
                WHEN t.pts > 20 THEN 'star'
                WHEN t.pts > 15 THEN 'good'
                WHEN t.pts > 10 THEN 'average'
                ELSE 'bad'
            END::scoring_class
        ELSE y.scoring_class
    END as scoring_class,

    -- Logic to track years of inactivity
    CASE
        WHEN t.season IS NOT NULL THEN 0 -- Reset counter if active
        ELSE y.years_since_last_season + 1 -- Increment if inactive
    END as years_since_last_season,

    -- Update the version tracker to the new season
    COALESCE(t.season, y.current_season + 1) as current_season

-- The join that makes it all possible
FROM today AS t
FULL OUTER JOIN yesterday AS y ON t.player_name = y.player_name;
```

### Explanation of Key Concepts

#### `WITH` Clause (Common Table Expressions - CTEs)
-   **`yesterday`**: Represents the state of our cumulative `players` table at the end of the previous season (2000). It's our historical baseline.
-   **`today`**: Represents the new, incoming data for the current season (2001) from a staging or source table (`player_seasons`).

#### `FULL OUTER JOIN`
This is the engine of the query. By joining `today` and `yesterday` on `player_name`, it correctly handles all three possible scenarios:
1.  **Inner Join part**: Players who exist in both tables (returning players who played last season and this season).
2.  **Left Join part**: Players who exist only in `today` (brand new players).
3.  **Right Join part**: Players who exist only in `yesterday` (players who were active last season but are inactive or retired this season).

#### `COALESCE(value1, value2)`
-   This function returns the first non-NULL value it finds in its argument list. We use it to merge player metadata. For example, `COALESCE(t.height, y.height)` means: "Use the height from the `today` table if it exists; otherwise, fall back to using the height from the `yesterday` table." This ensures we always have the most up-to-date information.

#### `CASE` Statements (The Logic Engine)
-   **Updating `season_stats`**: This is the most complex `CASE` statement.
    -   `WHEN y.season_stats IS NULL`: This handles new players (they only exist in `today`). We create a brand new array containing just their stats for the current season. `ROW(...)` constructs the `season_stats` object, and `::season_stats` casts it to the correct type.
    -   `WHEN t.season IS NOT NULL`: This handles returning players. We take their existing stats array (`y.season_stats`) and append (`||` operator) the new season's stats to it.
    -   `ELSE`: This handles inactive players (they only exist in `yesterday`). We simply carry forward their existing, unchanged `season_stats` array.
-   **Other `CASE` Statements**: The other `CASE` statements follow a similar logic: if there is new data in `today`, calculate a new value (for `scoring_class`, `years_since_last_season`); otherwise, carry forward the old value from `yesterday`.

---

## 3. Analytical Queries on the Cumulative Table

Now that we have this rich, historical table, we can ask interesting questions.

### Unnesting Historical Data

The `season_stats` column stores data in a compressed array format. To analyze it, we need to "unpack" or `unnest` it.

```sql
WITH unnested AS (
    SELECT
        player_name,
        unnest(season_stats)::season_stats as season_stats
    FROM players
    WHERE player_name = 'Michael Jordan' AND current_season = 2001
)
SELECT
    player_name,
    (season_stats::season_stats).* -- Expands the struct into columns
FROM unnested;
```
**Explanation:**
1.  **`unnest(season_stats)`**: This function takes the `season_stats` array and expands it, creating a separate row for each element in the array.
2.  **`WITH unnested AS (...)`**: We put the `unnest` operation in a CTE for clarity.
3.  **`(season_stats::season_stats).*`**: This special syntax takes the `season_stats` composite type object and expands its fields (`season`, `gp`, `pts`, etc.) into their own individual columns in the final result set.

This query transforms the compressed historical data for Michael Jordan into a readable, row-by-row format of his seasonal stats.

### Advanced Analysis: Performance Ratio

This query calculates the ratio of points scored in a player's most recent season compared to their first recorded season.

```sql
SELECT
    player_name,
    (season_stats[CARDINALITY(season_stats)]::season_stats).pts /
        CASE
            WHEN (season_stats[1]::season_stats).pts = 0 THEN 1
            ELSE (season_stats[1]::season_stats).pts
        END
FROM players
WHERE current_season = 2001 AND scoring_class = 'star'
ORDER BY 2 DESC;
```

**Explanation:**
-   **`CARDINALITY(season_stats)`**: This function returns the number of elements in the `season_stats` array.
-   **`season_stats[CARDINALITY(...)]`**: This accesses the *last* element of the array (the most recent season).
-   **`season_stats[1]`**: This accesses the *first* element of the array (the first recorded season).
-   **`(...).pts`**: After accessing an element from the array, this syntax retrieves the `pts` field from that `season_stats` object.
-   **`CASE ... END`**: This is a crucial safeguard to **prevent division-by-zero errors**. If a player's points in their first season were 0, we divide by 1 instead to avoid an error.

