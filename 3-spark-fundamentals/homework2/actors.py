from pyspark.sql import SparkSession

query = """
WITH deduped AS (
    SELECT
        *,
        row_number() over (PARTITION BY player_id, game_id ORDER BY pts DESC) AS row_num
    FROM game_details
)
SELECT
    player_id AS subject_identifier,
    'player' as subject_type,
    game_id AS object_identifier,
    'game' AS object_type,
    'plays_in' AS edge_type,
    map(
        'start_position', start_position,
        'pts', CAST(pts AS STRING),
        'team_id', CAST(team_id AS STRING),
        'team_abbreviation', team_abbreviation
    ) as properties
FROM deduped
WHERE row_num = 1
"""
def do_game_details_edge_transformation(spark, dataframe):
    dataframe.createOrReplaceTempView("game_details")
    return spark.sql(query)



def main():
    spark = SparkSession.builder \
        .master("local") \
        .appName("game_details_edge") \
        .getOrCreate()
    output_df = do_game_details_edge_transformation(spark, spark.table("game_details"))
    output_df.write.mode("overwrite").insertInto("game_details_edge")
