from pyspark.sql import SparkSession


query="""
WITH dedup_events AS (
SELECT
	USER_ID,
	DEVICE_ID,
	HOST,
	EVENT_TIME,
	ROW_NUMBER() OVER (
		PARTITION BY
			USER_ID,
			DEVICE_ID,
			HOST,
			EVENT_TIME ORDER BY EVENT_TIME DESC
	) AS ROW_NUM
		
from events
		
)

SELECT  USER_ID,
	DEVICE_ID,
	HOST,
	EVENT_TIME
		FROM  dedup_events 
        WHERE row_num=1

"""


def do_dedup_events_transformation(spark, dataframe):
    dataframe.createOrReplaceTempView("events")
    return spark.sql(query)



def main():
    spark = SparkSession.builder \
        .master("local") \
        .appName("events") \
        .getOrCreate()
    output_df = do_dedup_events_transformation(spark, spark.table("events"))
    output_df.write.mode("overwrite").insertInto("deduped_events")