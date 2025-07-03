/*********************************************************************************************
*********   Query 1 : A query to deduplicate game_details from Day 1 so there's no duplicates    *********
*********                                                                                        *********
*********                                                                                        *********
*********************************************************************************************/
-- A query to deduplicate game_details from Day 1 so there's no duplicates
WITH dedup AS (
    SELECT
        *, -- Select all columns from the game_details table
        -- Assign a row number to each record within partitions.
        -- A partition is defined by the unique combination of game_id, team_id, and player_id.
        -- This means for every duplicate set based on these three columns, rows will be numbered starting from 1.
        ROW_NUMBER() OVER(PARTITION BY game_id, team_id, player_id ORDER BY game_id) as row_no -- Added ORDER BY for deterministic row_number
    FROM game_details
)
-- Select only the first row from each partition (where row_no is 1).
-- This effectively removes duplicates, keeping just one record for each unique (game_id, team_id, player_id) combination.
SELECT * FROM dedup
WHERE row_no = 1;

/*********************************************************************************************
*********   Query 1 PART 2 : A query to deduplicate                                                *********
*********                  device from Day 1 so there's no duplicates                              *********
*********                                                                                          *********
*********************************************************************************************/
WITH dedup AS (
    SELECT
        *, -- Select all columns from the devices table
        -- Assign a row number to each record within partitions.
        -- Partitions are based on unique combinations of device_id, browser_type, and os_type.
        -- This helps identify and group potential duplicate device entries.
        ROW_NUMBER() OVER(PARTITION BY device_id, browser_type, os_type ORDER BY device_id) as row_num -- Added ORDER BY for deterministic row_number
    FROM devices
)
-- Perform a FULL OUTER JOIN between the deduplicated 'devices' (aliased as 'de')
-- and the 'events' table (aliased as 'e') based on device_id.
-- This join would typically be used to see all devices (deduplicated or not) and all events,
-- including those without a matching device or vice versa.
SELECT * FROM dedup de
FULL OUTER JOIN events e ON e.device_id = de.device_id
-- Filter the results to only include the first (deduplicated) row for each device combination from the 'devices' table.
WHERE de.row_num = 1;



/*********************************************************************************************
*********   Query 2 : A DDL for an user_devices_cumulated table                                  *********
*********                                                                                        *********
*********                                                                                        *********
*********************************************************************************************/

-- DDL (Data Definition Language) for the user_devices_cumulated table.
-- This table is designed to store cumulative user device activity.

CREATE TABLE user_devices_cumulated (
    user_id numeric,       -- Numeric identifier for the user.
    device_id numeric,     -- Numeric identifier for the device.
    browser_type TEXT,     -- Text field for the type of browser used (e.g., 'Chrome', 'Firefox').
    date DATE,             -- This column represents the snapshot date for the cumulative data.
    device_activity_datelist DATE[], -- An array (list) of DATEs, intended to store all active days
                                     -- for a given user, device, and browser type.

    -- Defines the composite primary key for the table.
    -- This constraint ensures that each combination of user_id, device_id, browser_type, and date is unique.
    PRIMARY KEY (user_id, device_id, browser_type, date)
);




/*********************************************************************************************
*********   Query 3 : Cumulative query to generate device_activity_datelist from events           *********
*********                                                                                        *********
*********                                                                                        *********
*********************************************************************************************/

-- Cumulative query to generate device_activity_datelist and load it into user_devices_cumulated.
-- This query processes daily event data to update or insert cumulative device activity.

INSERT INTO user_devices_cumulated
-- CTE to deduplicate records from the 'devices' table.
-- Ensures that only unique device entries (by device_id, browser_type, os_type) are considered.
WITH dedup_device AS (
    SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY device_id, browser_type, os_type ORDER BY device_id) as row_num
    FROM devices
),
-- CTE to deduplicate records from the 'events' table.
-- Ensures unique events based on user_id, device_id, host, and event_time.
dedup_events AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                USER_ID,
                DEVICE_ID,
                HOST,
                EVENT_TIME
            ORDER BY EVENT_TIME
        ) AS ROW_NUM
    FROM events
),
-- CTE to combine deduplicated device and event data.
-- It aims to get relevant user, device, browser, and event_time information.
dataset AS (
    SELECT
        de.user_id,
        dd.device_id,
        dd.browser_type, -- Using browser_type from dedup_device for device characteristics
        de.event_time
    FROM dedup_device dd
    -- FULL OUTER JOIN used here to merge device metadata with event data.
    -- The WHERE clause filters to only valid, deduplicated entries.
    FULL OUTER JOIN dedup_events de ON de.device_id = dd.device_id
    WHERE de.row_num = 1 -- Only consider the deduplicated event records
      AND dd.row_num = 1 -- Only consider the deduplicated device records
      AND de.user_id IS NOT NULL -- Exclude events without a user_id
),
-- CTE to retrieve the cumulative data from the 'user_devices_cumulated' table for the previous day.
-- This represents the state of cumulative data before today's processing.
yesterday AS (
    SELECT * FROM user_devices_cumulated
    WHERE date = DATE('2023-01-30') -- Assuming '2023-01-30' is yesterday's date for this run
),
-- CTE to identify unique user, device, browser activity for 'today' from the processed dataset.
-- Groups by user, device, browser, and active_date to ensure one entry per day for each combination.
today AS (
    SELECT
        user_id,
        device_id,
        browser_type,
        DATE(event_time) as active_date -- Extract the date of the event
    FROM dataset
    WHERE DATE(event_time) = DATE('2023-01-31') -- Filter for today's events ('2023-01-31')
    GROUP BY 1, 2, 3, 4 -- Group by user_id, device_id, browser_type, and active_date
)
-- Main SELECT statement to prepare data for insertion/update into 'user_devices_cumulated'.
SELECT
    -- COALESCE is used to pick the user_id from 'today' if available, otherwise from 'yesterday'.
    -- This handles new entries or entries that were only active yesterday.
    COALESCE(t.user_id, y.user_id) as user_id,
    COALESCE(t.device_id, y.device_id) as device_id,
    COALESCE(t.browser_type, y.browser_type) as browser_type,
    -- Determine the 'date' for the current cumulative record.
    -- If 'today' has an active_date, use it. Otherwise, assume it's yesterday's date plus one day.
    DATE(COALESCE(t.active_date, y.date + INTERVAL '1 day')) as date,
    -- Logic to construct the device_activity_datelist array:
    CASE
        -- If there's no existing cumulative list for this combination (i.e., new entry), start with today's active_date.
        WHEN y.device_activity_datelist IS NULL THEN ARRAY[t.active_date]
        -- If there's no activity today for this combination (t.active_date is NULL), keep yesterday's list as is.
        WHEN t.active_date IS NULL THEN y.device_activity_datelist
        -- If both exist (active today and was active yesterday), concatenate today's date to yesterday's list.
        ELSE ARRAY[t.active_date] || y.device_activity_datelist
    END as device_activity_datelist
FROM today t
-- FULL OUTER JOIN to combine today's activity with yesterday's cumulative data.
-- This ensures all user/device/browser combinations from both days are considered.
FULL OUTER JOIN yesterday y
ON t.user_id = y.user_id
   AND t.browser_type = y.browser_type
   AND t.device_id = y.device_id;



/*********************************************************************************************
*********   Query 4 : A datelist_int generation query.                                           *********
*********      Convert the device_activity_datelist column into a datelist_int column             *********
*********                                                                                        *********
*********************************************************************************************/


-- A query to generate a 'datelist_int' (bitmask) from the 'device_activity_datelist' array.
-- This converts an array of dates into a single integer where each bit represents a day of the month.

WITH users AS (
    -- Select the cumulative user device data for a specific date (e.g., end of month).
    -- This provides the 'device_activity_datelist' array for each user/device/browser.
    SELECT * FROM user_devices_cumulated
    WHERE date = DATE('2023-01-31')
),
series AS (
    -- Generate a series of all dates within the month for which we are creating the bitmask.
    -- This will be used to check each day's presence in 'device_activity_datelist'.
    SELECT generate_series('2023-01-01', '2023-01-31', INTERVAL '1 day') AS series_date
),
place_holder_ints AS (
    -- CROSS JOIN 'users' with 'series' to get every user/device/browser combination for every day in the month.
    -- Then calculate a bit value for each day if it's present in the 'device_activity_datelist'.
    SELECT
        -- If the 'device_activity_datelist' array contains the current 'series_date',
        -- calculate a power of 2 for that day.
        -- The power is determined by 32 minus the day-of-month offset (e.g., day 1 = bit 31, day 31 = bit 1).
        -- This creates a bitmask where a set bit indicates activity on that day.
        CASE WHEN device_activity_datelist @> ARRAY[DATE(series_date)]
            THEN CAST(POW(2, 32 - (series_date - DATE_TRUNC('month', series_date) + 1)) AS BIGINT) -- Adjusted calculation for POW and included +1 for 1-based day
            ELSE 0 -- If no activity on this series_date, the bit value is 0.
        END as placeholder_int_value,
        users.user_id,        -- Include key columns from the 'users' CTE
        users.device_id,
        users.browser_type,
        users.date,           -- The snapshot date from user_devices_cumulated
        series.series_date    -- The individual date from the generated series
    FROM users CROSS JOIN series
)
-- Final selection to aggregate the bit values into a single bitmask per user/device/browser.
SELECT
    user_id,
    device_id,
    browser_type,
    -- Sum all 'placeholder_int_value' (powers of 2) for each group to get the final bitmask.
    -- Cast it to BIGINT first (as SUM returns NUMERIC) and then to BIT(32).
    CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32)) AS datelist_int,
    -- Calculate 'dim_is_monthly_active': True if any bit is set in the bitmask, indicating activity in the month.
    BIT_COUNT(CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 AS dim_is_monthly_active,
    -- Calculate 'dim_is_weekly_active': True if any activity occurred within the last 7 days.
    -- This uses a fixed mask '1111111000000000000000000000000' (7 most significant bits).
    BIT_COUNT(CAST('1111111000000000000000000000000' AS BIT(32)) &
        CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 AS dim_is_weekly_active,
    -- Calculate 'dim_is_daily_active': True if active on the current day (usually the last day of the month/period).
    -- This uses a mask '100000000000000000000000000000' (the most significant bit, corresponding to Day 1 based on original logic).
    -- If daily active means active on the *current processing day*, the mask needs to be dynamically adjusted based on (day_of_month - 1)
    -- or the specific bit representing the end of the month.
    BIT_COUNT(CAST('100000000000000000000000000000' AS BIT(32)) &
        CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 AS dim_is_daily_active
FROM place_holder_ints
GROUP BY user_id, device_id, browser_type;



/*********************************************************************************************
*********   Query 5 : A DDL for hosts_cumulated table                                          *********
*********                                                                                        *********
*********                                                                                        *********
*********************************************************************************************/


-- DDL for the hosts_cumulated table.
-- Similar to user_devices_cumulated, this table stores cumulative host activity.

CREATE TABLE hosts_cumulated (
    host TEXT,                 -- Text field for the host identifier.
    date DATE,                 -- Snapshot date for the cumulative data.
    host_activity_datelist DATE[], -- An array of DATEs, tracking all active days for a given host.

    -- Defines the composite primary key.
    PRIMARY KEY (host, date)
);


/*********************************************************************************************
*********   Query 6 : The incremental query to generate host_activity_datelist                   *********
*********                                                                                        *********
*********                                                                                        *********
*********************************************************************************************/

-- Incremental query to generate and update host_activity_datelist in hosts_cumulated.
-- This processes daily event data to build the cumulative host activity list.

INSERT INTO hosts_cumulated
-- CTE to retrieve cumulative host data from the 'hosts_cumulated' table for the previous day.
-- This is the 'old' state to be merged with today's data.
WITH yesterday AS (
    SELECT * FROM hosts_cumulated
    WHERE date = DATE('2023-01-03') -- Assuming '2023-01-03' is yesterday's date for this run
),
-- CTE to identify unique host activity for 'today' from the 'events' table.
-- Groups by host and active_date to get one entry per host per day.
today AS (
    SELECT
        host,
        DATE(event_time) as active_date -- Extract the date of the event
    FROM events
    WHERE DATE(event_time) = DATE('2023-01-04') -- Filter for today's events ('2023-01-04')
    GROUP BY host, DATE(event_time)
)
-- Main SELECT statement to prepare data for insertion/update into 'hosts_cumulated'.
SELECT
    -- COALESCE to get the host from 'today' if active, otherwise from 'yesterday'.
    COALESCE(t.host, y.host) as host,
    -- Determine the 'date' for the current cumulative record.
    -- If active today, use today's date. Otherwise, use yesterday's date + 1 day to mark progression.
    DATE(COALESCE(t.active_date, y.date + INTERVAL '1 day')) as date,
    -- Logic to construct the host_activity_datelist array:
    CASE
        -- If there's an existing list from yesterday, append today's active_date to it.
        WHEN y.host_activity_datelist IS NOT NULL THEN y.host_activity_datelist || ARRAY[t.active_date]
        -- If there was no activity today for this host, propagate yesterday's list.
        WHEN t.active_date IS NULL THEN y.host_activity_datelist
        -- If it's a new host or active today but not yesterday, start a new list with today's date.
        ELSE ARRAY[t.active_date]
    END as host_activity_datelist
FROM today as t
-- FULL OUTER JOIN to combine today's activity with yesterday's cumulative data.
-- This ensures all hosts from both 'today' and 'yesterday' are included in the result.
FULL OUTER JOIN yesterday as y
ON t.host = y.host;



/*********************************************************************************************
*********   Query 7 : A monthly, reduced fact table DDL host_activity_reduced                    *********
*********                                                                                        *********
*********                                                                                        *********
*********************************************************************************************/

-- DDL for the hosts_activity_reduced table.
-- This table is designed as a monthly, reduced fact table for host activity.

CREATE TABLE hosts_activity_reduced (
    host TEXT,                       -- Text field for the host identifier.
    month_start DATE,                -- The first day of the month for which the data is aggregated.
    hit_array REAL[],                -- An array to store daily 'hit' counts for the month.
    unique_visitors REAL[],          -- An array to store daily 'unique visitor' counts for the month.
    -- Defines the composite primary key for the table.
    -- This ensures a unique row for each host for each month, which is appropriate for a monthly aggregate.
    PRIMARY KEY (host, month_start)
);

/*********************************************************************************************
*********   Query 8 : An incremental query that loads host_activity_reduced day-by-day           *********
*********                                                                                        *********
*********                                                                                        *********
*********************************************************************************************/


-- Incremental query to load and update host_activity_reduced table day-by-day.
-- This query aggregates daily event data into monthly arrays for hits and unique visitors.

INSERT INTO hosts_activity_reduced
-- CTE to calculate daily aggregates (site hits and unique visitors) for a specific day.
WITH daily_aggregate AS (
    SELECT
        host,
        DATE(event_time) as date,       -- The date of the daily activity
        Count(1) as site_hits,          -- Total number of hits for the host on this date
        COUNT(DISTINCT user_id) as unique_visitors -- Number of unique users for the host on this date
    FROM events
    WHERE DATE(event_time) = DATE('2023-01-05') -- Filter for the current day's events ('2023-01-05')
    GROUP BY host, DATE(event_time) -- Group to get daily aggregates per host
),
-- CTE to retrieve the existing monthly cumulative data from 'hosts_activity_reduced'.
-- Filters for the 'month_start' that corresponds to the current processing month.
yesterday AS (
    SELECT * FROM hosts_activity_reduced
    WHERE month_start = DATE('2023-01-01') -- Assuming '2023-01-01' is the start of the current month
)
-- Main SELECT statement to prepare data for insertion/update.
SELECT
    -- COALESCE to select the host identifier, preferring today's aggregate if available.
    COALESCE(da.host, y.host) as host,
    -- COALESCE to select the month_start, preferring yesterday's if available,
    -- otherwise deriving it from today's date (truncating to month start).
    COALESCE(y.month_start, DATE_TRUNC('month', da.date)) as month_start,
    -- Logic to construct the 'hit_array':
    CASE
        -- If there's an existing 'hit_array' from 'yesterday', append today's site_hits.
        -- COALESCE(da.site_hits, 0) ensures a 0 is appended if no hits today (for consistent array length).
        WHEN y.hit_array IS NOT NULL THEN
            y.hit_array || ARRAY[COALESCE(da.site_hits, 0)]
        -- If there's no existing 'hit_array' (new host/month combination),
        -- create a new array.
        WHEN y.hit_array IS NULL THEN
            -- ARRAY_FILL(0, ARRAY[day_offset]) creates an array of zeros for days before today in the month.
            -- This fills the leading empty slots.
            ARRAY_FILL(0, ARRAY[COALESCE(da.date - DATE(DATE_TRUNC('month', da.date)), 0)])
            -- Concatenate today's site_hits to the end of the newly created (or filled) array.
            || ARRAY[COALESCE(da.site_hits, 0)]
    END as hit_array,
    -- Logic to construct the 'unique_visitors' array (identical logic to 'hit_array'):
    CASE
        WHEN y.unique_visitors IS NOT NULL THEN
            y.unique_visitors || ARRAY[COALESCE(da.unique_visitors, 0)]
        WHEN y.unique_visitors IS NULL THEN
            ARRAY_FILL(0, ARRAY[COALESCE(da.date - DATE(DATE_TRUNC('month', da.date)), 0)])
            || ARRAY[COALESCE(da.unique_visitors, 0)]
    END as unique_visitors
FROM daily_aggregate as da
-- FULL OUTER JOIN to merge today's daily aggregates with yesterday's monthly cumulated data.
-- This handles hosts active today, hosts active previously but not today, and new hosts.
FULL OUTER JOIN yesterday y ON y.host = da.host
-- ON CONFLICT clause to handle cases where a record for (host, month_start) already exists.
-- This ensures the query is idempotent (can be run multiple times without creating duplicates or errors).
ON CONFLICT (host, month_start)
DO
    UPDATE SET
        -- If a conflict occurs, update the existing 'hit_array' with the one calculated by the SELECT statement.
        hit_array = EXCLUDED.hit_array,
        -- Update the existing 'unique_visitors' array similarly.
        unique_visitors = EXCLUDED.unique_visitors;