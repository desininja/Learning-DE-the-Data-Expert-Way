
WITH users AS (
SELECT * FROM users_cumulated
WHERE date = DATE('2023-01-31')

),

series AS(


SELECT generate_series('2023-01-01', '2023-01-31', INTERVAL '1 day') AS series_date
),

place_holder_ints AS (
select 
		CASE WHEN
			dates_active @> ARRAY [DATE(series_date)] 
			THEN CAST(POW(2,32-(date - DATE(series_date))) as BIGINT)
				ELSE 0
					END as placeholder_int_value,
		*

from users  CROSS JOIN series
--where user_id ='137925124111668560'
)


select user_id,
		CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32)),
		BIT_COUNT(CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 as dim_is_monthly_active,
		BIT_COUNT(CAST('1111111000000000000000000000000' AS BIT(32)) &
			CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 as dim_is_weekly_active,
		BIT_COUNT(CAST('100000000000000000000000000000' AS BIT(32)) &
			CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 as dim_is_daily_active
from place_holder_ints
GROUP BY user_id





