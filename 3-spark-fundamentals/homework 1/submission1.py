import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

// Initialize SparkSession (assuming it's already created elsewhere or create here)
// val spark = SparkSession.builder().appName("GameDataAnalysis").getOrCreate()

// Configure Spark settings for performance
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1") // Disable auto broadcast join to control manually
spark.conf.set("spark.sql.analyzer.failAmbiguousSelfJoin", "false") // Allow ambiguous self-joins (be cautious)
spark.conf.set("spark.sql.shuffle.partitions", "16") // Set shuffle partitions for better parallelism

// Define batch_number for bucketing
val batch_number = 16

// Load data from CSV files into DataFrames
val maps = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/maps.csv")
val medals = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/medals.csv")
val match_details = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/match_details.csv")
val matches = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/matches.csv")
val medal_matches_player = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/medals_matches_players.csv")

// Drop existing Iceberg tables if they exist
spark.sql("""DROP TABLE IF EXISTS bootcamp.maps""") // Maps and Medals are not bucketed here
spark.sql("""DROP TABLE IF EXISTS bootcamp.medals""")
spark.sql("""DROP TABLE IF EXISTS bootcamp.match_details_bucketed""")
spark.sql("""DROP TABLE IF EXISTS bootcamp.matches_bucketed""")
spark.sql("""DROP TABLE IF EXISTS bootcamp.medal_matches_player_bucketed""")

// Define DDL for bucketing tables by match_id
val matchDetailsBucketedDDL = s"""
CREATE TABLE IF NOT EXISTS bootcamp.match_details_bucketed (
  match_id STRING, player_gamertag STRING, previous_spartan_rank INTEGER, spartan_rank INTEGER,
  previous_total_xp INTEGER, total_xp INTEGER, previous_csr_tier INTEGER, previous_csr_designation INTEGER,
  previous_csr INTEGER, previous_csr_percent_to_next_tier INTEGER, previous_csr_rank INTEGER,
  current_csr_tier INTEGER, current_csr_designation INTEGER, current_csr INTEGER,
  current_csr_percent_to_next_tier INTEGER, current_csr_rank INTEGER, player_rank_on_team INTEGER,
  player_finished BOOLEAN, player_average_life STRING, player_total_kills INTEGER,
  player_total_headshots INTEGER, player_total_weapon_damage DOUBLE, player_total_shots_landed INTEGER,
  player_total_melee_kills INTEGER, player_total_melee_damage DOUBLE, player_total_assassinations INTEGER,
  player_total_ground_pound_kills INTEGER, player_total_shoulder_bash_kills INTEGER,
  player_total_grenade_damage DOUBLE, player_total_power_weapon_damage DOUBLE,
  player_total_power_weapon_grabs INTEGER, player_total_deaths INTEGER, player_total_assists INTEGER,
  player_total_grenade_kills INTEGER, did_win INTEGER, team_id INTEGER
) USING iceberg
PARTITIONED BY (bucket(${batch_number}, match_id));
"""

val matchesBucketedDDL = s"""
CREATE TABLE IF NOT EXISTS bootcamp.matches_bucketed (
  match_id STRING, mapid STRING, is_team_game BOOLEAN, playlist_id STRING, game_variant_id STRING,
  is_match_over BOOLEAN, completion_date TIMESTAMP, match_duration STRING, game_mode STRING, map_variant_id STRING
) USING iceberg
PARTITIONED BY (bucket(${batch_number}, match_id));
"""

val medalMatchesPlayerBucketedDDL = s"""
CREATE TABLE IF NOT EXISTS bootcamp.medal_matches_player_bucketed (
  match_id STRING, player_gamertag STRING, medal_id LONG, count INTEGER
) USING iceberg
PARTITIONED BY (bucket(${batch_number}, match_id));
"""

// Create the bucketed Iceberg tables
spark.sql(matchDetailsBucketedDDL)
spark.sql(matchesBucketedDDL)
spark.sql(medalMatchesPlayerBucketedDDL)

// Write data into the newly created bucketed tables
match_details.select(
  $"match_id", $"player_gamertag", $"previous_spartan_rank", $"spartan_rank",
  $"previous_total_xp", $"total_xp", $"previous_csr_tier", $"previous_csr_designation",
  $"previous_csr", $"previous_csr_percent_to_next_tier", $"previous_csr_rank",
  $"current_csr_tier", $"current_csr_designation", $"current_csr",
  $"current_csr_percent_to_next_tier", $"current_csr_rank", $"player_rank_on_team",
  $"player_finished", $"player_average_life", $"player_total_kills",
  $"player_total_headshots", $"player_total_weapon_damage", $"player_total_shots_landed",
  $"player_total_melee_kills", $"player_total_melee_damage", $"player_total_assassinations",
  $"player_total_ground_pound_kills", $"player_total_shoulder_bash_kills",
  $"player_total_grenade_damage", $"player_total_power_weapon_damage",
  $"player_total_power_weapon_grabs", $"player_total_deaths", $"player_total_assists",
  $"player_total_grenade_kills", $"did_win", $"team_id"
).write.mode("overwrite").bucketBy(batch_number,"match_id").saveAsTable("bootcamp.match_details_bucketed")

matches.select(
  $"match_id", $"mapid", $"is_team_game", $"playlist_id", $"game_variant_id",
  $"is_match_over", $"completion_date", $"match_duration", $"game_mode", $"map_variant_id"
).write.mode("overwrite").bucketBy(batch_number,"match_id").saveAsTable("bootcamp.matches_bucketed")

medal_matches_player.select(
  $"match_id", $"player_gamertag", $"medal_id", $"count"
).write.mode("overwrite").bucketBy(batch_number,"match_id").saveAsTable("bootcamp.medal_matches_player_bucketed")

// Join bucketed tables for efficient match_id joins
val df = spark.sql("""
  SELECT *
  FROM bootcamp.matches_bucketed mb
  JOIN bootcamp.medal_matches_player_bucketed mmpb ON mmpb.match_id = mb.match_id
  JOIN bootcamp.match_details_bucketed mdb ON mdb.match_id = mb.match_id
""")

df.show(5)

// Join with medals DataFrame (smaller, broadcastable)
val medal = medals.withColumnRenamed("name","medal_name")
val joinedDfMedals = df.join(
  broadcast(medal), // Broadcast medals for efficient join
  df("medal_id") === medal("medal_id"), // Use medal for column reference as it's the right side of the join
  "inner"
)

joinedDfMedals.printSchema()

// Join with maps DataFrame (smaller, broadcastable)
val finalDF = joinedDfMedals.join(
  broadcast(maps), // Broadcast maps for efficient join
  joinedDfMedals("mapid") === maps("mapid"),
  "inner"
)

finalDF.printSchema()

// Which player averages the most kills per game?
val aggKills = finalDF.groupBy("mdb.player_gamertag","mb.match_id").agg(sum("player_total_kills").alias("kills_this_game"))
val avgKillsPerPlayer = aggKills
  .groupBy("player_gamertag")
  .agg(avg("kills_this_game").alias("avg_kills_per_game"))
  .orderBy(col("avg_kills_per_game").desc)
  .limit(1)

avgKillsPerPlayer.show()

// Which playlist gets played the most?
val mostPlayedPlaylist = finalDF
  .groupBy("playlist_id")
  .agg(countDistinct("mb.match_id").alias("match_count"))
  .orderBy(col("match_count").desc)
  .limit(1)

println("Playlist played the most:")
mostPlayedPlaylist.show()

// Which map gets played the most?
val mostPlayedMap = finalDF
    .groupBy("mb.mapid")
    .agg(countDistinct("mb.match_id").alias("match_count"))
    .orderBy(col("match_count").desc)
    .limit(1)

println("Map played the most:")
mostPlayedMap.show()

// Which map do players get the most Killing Spree medals on?
val killingSpreeDF = finalDF.filter(col("medal_name")==="Killing Spree")
killingSpreeDF.groupBy("mb.mapid").agg(sum("count").alias("killingSpreeCount"))
.orderBy(col("killingSpreeCount").desc).limit(1).show()