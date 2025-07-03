select * from devices;

select * from events;

select MIN(DATE(event_time)), MAX(DATE(event_time)) from events;
/*
MIN					MAX
"2023-01-01"	"2023-01-31"
*/
SELECT * FROM devices
order by device_id;
WHERE device_id IN ( )'4523912434586500000'

---------------------------------------------------------------------------------------------------------
-- A query to deduplicate game_details from Day 1 so there's no duplicates
WITH dedup AS (

SELECT 
		*,
		ROW_NUMBER() OVER(PARTITION BY game_id,team_id,player_id) as row_no
FROM game_details
)
SELECT * FROM dedup
WHERE row_no = 1
---------------------------------------------------------------------------------------------------------

SELECT device_id,
		browser_type,
		os_type,
		count(1)
FROM devices
GROUP BY 1,2,3
ORDER BY 4 DESC;


-------------
3887



SELECT * FROM devices
WHERE device_id IN (select device_id from( SELECT device_id,
		count(1) as device_cnt
FROM devices
GROUP BY 1
) as temp where device_cnt = 2) order by device_id




------------------------------------------------
-- A query to deduplicate game_details from Day 1 so there's no duplicates
WITH dedup AS (
SELECT *,
		ROW_NUMBER() OVER(PARTITION BY device_id, browser_type, os_type ) as row_num
FROM devices
)

SELECT * FROM dedup de
FULL OUTER JOIN events e ON e.device_id =de.device_id
WHERE de.row_num = 1;




-- DDL for device_activity_datelist

-- SELECT * FROM events; --For analysis purpose.
-- DROP TABLE device_activity_datelist;
-- DROP TABLE user_devices_cumulated;
CREATE TABLE user_devices_cumulated (
	user_id numeric,
	device_id numeric,
	browser_type TEXT,
	date DATE,
	device_activity_datelist DATE[],

	PRIMARY KEY (user_id,device_id,browser_type,date)

)



-- cumulative query to generate device_activity_datelist from events


WITH dedup_device AS (
SELECT *,
		ROW_NUMBER() OVER(PARTITION BY device_id, browser_type, os_type ) as row_num
FROM devices
),

dedup_events AS (
SELECT
	*,
	ROW_NUMBER() OVER (
		PARTITION BY
			USER_ID,
			DEVICE_ID,
			HOST,
			EVENT_TIME
	) AS ROW_NUM
		
from events
		
),
dataset AS (
SELECT  de.user_id,
		dd.device_id,
		browser_type,
		event_time 
		FROM dedup_device dd
FULL OUTER JOIN dedup_events de ON de.device_id =dd.device_id
WHERE de.row_num = 1 AND dd.row_num=1
AND de.user_id IS NOT NULL),

yesterday  AS (
SELECT * FROM user_devices_cumulated
WHERE date = DATE('2023-01-02')
),
today AS (

SELECT 
		user_id,
		device_id,
		browser_type,
		DATE(event_time) as active_date
		FROM dataset
		WHERE DATE(event_time) = DATE('2023-01-03')
		GROUP BY 1,2,3,4
)

SELECT
		COALESCE(t.user_id,y.user_id) as user_id,
		COALESCE(t.device_id,y.device_id) as device_id,
		COALESCE(t.browser_type, y.browser_type) as browser_type,
		DATE(COALESCE(t.active_date,y.date + INTERVAL '1 day')) as date,
		CASE
			WHEN y.device_activity_datelist  IS NULL
			THEN ARRAY[t.active_date]
			WHEN t.active_date IS NULL THEN y.device_activity_datelist
			ELSE ARRAY[t.active_date] || y.device_activity_datelist
			END as device_activity_datelist
from today t 
FULL outer join yesterday y 
on t.user_id = y.user_id AND t.browser_type = y.browser_type AND t.device_id =y.device_id
WHERE t.user_id = 8045844334478885000


---------

WITH dedup AS (
SELECT *,
		ROW_NUMBER() OVER(PARTITION BY device_id, browser_type, os_type ) as row_num
FROM devices
)

SELECT --e.user_id,
		de.device_id,
		browser_type--,
--		event_time 
		FROM dedup de
--FULL OUTER JOIN events e ON e.device_id =de.device_id
WHERE de.row_num = 1
--AND user_id IS NOT NULL
order by device_id




WITH dedup AS (
SELECT *,
		ROW_NUMBER() OVER(PARTITION BY device_id, browser_type, os_type ) as row_num
FROM devices
)

SELECT e.user_id,
		de.device_id,
		browser_type,
		event_time,
		count(1) as cnt
		FROM dedup de
FULL OUTER JOIN events e ON e.device_id =de.device_id
WHERE de.row_num = 1
AND user_id IS NOT NULL
GROUP BY 1,2,3,4
ORDER BY 5 DESC



--------------------------------------------------------------------------------

select 
		*,
		ROW_NUMBER() OVER(PARTITION BY user_id,device_id, host, event_time ) as row_num
		
from events





-----------
SELECT * FROM
user_devices_cumulated
WHERE USER_ID =8045844334478885000
date = DATE('2023-01-02') 

-----------------------


select host, count(1) from events
group by 1;



INSERT INTO user_devices_cumulated
WITH dedup_device AS (
SELECT *,
		ROW_NUMBER() OVER(PARTITION BY device_id, browser_type, os_type ) as row_num
FROM devices
),

dedup_events AS (
SELECT
	*,
	ROW_NUMBER() OVER (
		PARTITION BY
			USER_ID,
			DEVICE_ID,
			HOST,
			EVENT_TIME
	) AS ROW_NUM
		
from events
		
),
dataset AS (
SELECT  de.user_id,
		dd.device_id,
		browser_type,
		event_time 
		FROM dedup_device dd
FULL OUTER JOIN dedup_events de ON de.device_id =dd.device_id
WHERE de.row_num = 1 AND dd.row_num=1
AND de.user_id IS NOT NULL),

yesterday  AS (
SELECT * FROM user_devices_cumulated
WHERE date = DATE('2023-01-30')
),
today AS (

SELECT 
		user_id,
		device_id,
		browser_type,
		DATE(event_time) as active_date
		FROM dataset
		WHERE DATE(event_time) = DATE('2023-01-31')
		GROUP BY 1,2,3,4
)

SELECT
		COALESCE(t.user_id,y.user_id) as user_id,
		COALESCE(t.device_id,y.device_id) as device_id,
		COALESCE(t.browser_type, y.browser_type) as browser_type,
		DATE(COALESCE(t.active_date,y.date + INTERVAL '1 day')) as date,
		CASE
			WHEN y.device_activity_datelist  IS NULL
			THEN ARRAY[t.active_date]
			WHEN t.active_date IS NULL THEN y.device_activity_datelist
			ELSE ARRAY[t.active_date] || y.device_activity_datelist
			END as device_activity_datelist
from today t 
FULL outer join yesterday y 
on t.user_id = y.user_id AND t.browser_type = y.browser_type AND t.device_id =y.device_id

--TRUNCATE TABLE user_devices_cumulated



--A datelist_int generation query. Convert the device_activity_datelist column into a datelist_int column

WITH users AS (
SELECT * FROM user_devices_cumulated
WHERE date = DATE('2023-01-31')
),

series AS (
SELECT generate_series('2023-01-01', '2023-01-31', INTERVAL '1 day') AS series_date
),
place_holder_ints AS (
SELECT 
		CASE WHEN device_activity_datelist @>ARRAY[DATE(series_date)]
			THEN CAST(POW(2,32-(date-DATE(series_date))) as BIGINT)
			ELSE 0
			END as placeholder_int_value,

			*

FROM users CROSS JOIN series
		
)
select user_id,
		device_id,
		browser_type,
		CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32)),
		BIT_COUNT(CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 as dim_is_monthly_active,
		BIT_COUNT(CAST('1111111000000000000000000000000' AS BIT(32)) &
			CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 as dim_is_weekly_active,
		BIT_COUNT(CAST('100000000000000000000000000000' AS BIT(32)) &
			CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 as dim_is_daily_active
from place_holder_ints
GROUP BY user_id,device_id,browser_type




CREATE TABLE array_metrics (
user_id NUMERIC,
month_start DATE,
metric_name TEXT,
metric_array REAL[],
Primary KEy (user_id, month_start,metric_name)
)

select * from array_metrics

INSERT INTO array_metrics
wITH daily_Aggregate AS (
select user_id,
DATE(event_time) AS date,
count(1) as num_site_hits
FROM events
WHERE DATE(event_time) =DATE('2023-01-05') and user_id is not null
group by user_id,DATE(event_time)
),
yesterday_Array AS(
select * from array_metrics
where month_start = DATE('2023-01-01')
)

SELECT 
		coalesce(da.user_id,ya.user_id) as user_id,
		coalesce(ya.month_start,DATE_trunc('month',da.date)) as month_Start,
		'site_hits' as metric_name,
		cASE when ya.metric_Array is not null then
			ya.metric_array || ARRAY[COALESCE(da.num_site_hits,0)]
		WHEN ya.metric_Array is null then array_fill(0,Array[COALESCE(date-DATE(DATE_TRUNC('month',date)),0)])
		|| ARRAY[COALESCE(da.num_site_hits,0)]
		END as metric_Array

FROM daily_aggregate da
full outer join yesterday_array ya on 
da.user_id = ya.user_id
on conflict (user_id,month_Start,metric_name)
DO
	UPDATE SET metric_array = Excluded.metric_array;


truncate table array_metrics;


SELECT cardinality(metric_array), count(1)
FROM array_metrics
GROUP BY 1


WITH agg AS (

SELECT metric_name,
		month_Start,
		array[sum(metric_array[1]),
						sum(metric_array[2]),
						sum(metric_array[3]),
						sum(metric_array[4]),
						sum(metric_array[5])] as summed_array
FROM array_metrics
group by metric_name,month_start)


SELECT metric_name, 
		month_Start + CAST(CAST(index -1 AS TEXT)|| 'day' AS INTERVAL),
		elem as value

FROM agg
	CROSS JOIN unnest(agg.summed_array)
		WITH ORDINALITY AS a(elem,index)
;


select * from events;



"host"	"month_start"	"hit_array"	"unique_visitors"
"admin.zachwilson.tech"	"2023-01-01 00:00:00+00"	{74}	{10}
"www.eczachly.com"	"2023-01-01 00:00:00+00"	{100}	{21}
"www.zachwilson.tech"	"2023-01-01 00:00:00+00"	{262}	{53}


-- A monthly, reduced fact table DDL host_activity_reduced

CREATE TABLE hosts_activity_reduced (
		host TEXT,
		month_start DATE,
		hit_array REAl[],
		unique_visitors REAL[],
		Primary KEy (host,month_start)
)

--For analysis Purpose:
SELECT * from hosts_activity_reduced;
-- An incremental query that loads host_activity_reduced

INSERT INTO hosts_activity_reduced
WITH daily_aggregate AS (
SELECT host,
		DATE(event_time) as date,
		Count(1) as site_hits,
		COUNT(DISTINCT user_id) as unique_visitors

FROM events
WHERE DATE(event_time) = DATE('2023-01-05')
GROUP BY host,DATE(event_time)
),
yesterday AS (
select * from hosts_activity_reduced
where month_start = DATE('2023-01-01')
)

SELECT 
		COALESCE(da.host,y.host) as host,
		COALESCE(y.month_start,DATE_TRUNC('month',da.date)) as month_start,
		CASE WHEN y.hit_array IS NOT NULL THEN
					y.hit_array || ARRAY[COALESCE(da.site_hits,0)]
			 WHEN y.hit_array IS NULL THEN
					ARRAY_FILL(0,ARRAY[COALESCE(date - DATE(DATE_TRUNC('month',date)),0)])
					|| ARRAY[COALESCE(da.site_hits,0)]
		END as hit_array,
		CASE WHEN y.unique_visitors IS NOT NULL THEN
					y.unique_visitors || ARRAY[COALESCE(da.unique_visitors,0)]
			 WHEN y.unique_visitors IS NULL THEN
					ARRAY_FILL(0,ARRAY[COALESCE(date - DATE(DATE_TRUNC('month',date)),0)])
					|| ARRAY[COALESCE(da.unique_visitors,0)]
		END as unique_visitors		

FROM daily_aggregate as da 
FULL OUTER JOIN yesterday y ON y.host = da.host 
ON CONFLICT (host,month_start)
DO
	UPDATE SET hit_array = Excluded.hit_array,
				unique_visitors =  Excluded.unique_visitors;



-- A DDL for hosts_cumulated table

CREATE TABLE hosts_cumulated (
	host TEXT,
	date DATE,
	host_activity_datelist DATE[],

	PRIMARY KEY (host,date)

)

select * from events;
select * from hosts_Cumulated;


-- The incremental query to generate host_activity_datelist
INSERT INTO hosts_cumulated
WITH yesterday AS (
SELECT * FROM hosts_cumulated
WHERE date = DATE('2023-01-03')),

today AS (
SELECT 
		host,
		DATE(event_time) as active_date
FROM events
WHERE DATE(event_time) = DATE('2023-01-04')
GROUP BY host, DATE(event_time)
)

SELECT 
		COALESCE(t.host,y.host) as host,
		DATE(COALESCE(t.active_date, y.date + INTERVAL '1 day'))as DATE,
		CASE WHEN y.host_activity_datelist IS NOT NULL
			 THEN y.host_activity_datelist || ARRAY[t.active_date]
			 WHEN t.active_date IS NULL 
			 THEN y.host_activity_datelist
			 ELSE ARRAY[t.active_date]
			 END as host_activity_datelist
		
FROM today as t 
FULL OUTER JOIN yesterday as y 
ON t.host = y.host