# Databricks notebook source
# MAGIC %run "./HelperNotebooks/MountDataLakeStorage"

# COMMAND ----------

dbutils.widgets.text("DeltaPath", "/mnt/datalake/Silver/Simpsons")
dbutils.widgets.text("OutputPath", "/mnt/datalake/Gold/Simpsons")
delta_path = dbutils.widgets.get("DeltaPath")
output_path = dbutils.widgets.get("OutputPath")

# COMMAND ----------

def read_delta_to_dataframe(delta_location):
    df = spark.read.format("delta").load(delta_location)
    return df

# COMMAND ----------

character_df = read_delta_to_dataframe(f"{delta_path}/Characters")
episode_df = read_delta_to_dataframe(f"{delta_path}/Episodes")
location_df = read_delta_to_dataframe(f"{delta_path}/Locations")
scriptline_df = read_delta_to_dataframe(f"{delta_path}/ScriptLines")

# COMMAND ----------

episode_query = f'''
SELECT 
  ID as EpisodeID,
  image_url as ImageURL,
  imdb_rating as IMDBRating,
  imdb_votes as IMDBVotes,
  number_in_season as EpisodeNumInSeason,
  CAST(original_air_date as DATE) as OriginalAirDate,
  season as Season,
  title as EpisodeTitle
FROM delta.`{delta_path}/Episodes`
ORDER BY Season, EpisodeNumInSeason
'''

episode_df = spark.sql(episode_query)

# COMMAND ----------

character_df = spark.sql(f"SELECT id as CharacterID, UPPER(name) as CharacterName FROM delta.`{delta_path}/Characters` ORDER BY CharacterID")

# COMMAND ----------

location_df = spark.sql(f"SELECT id as LocationID, UPPER(name) as LocationName FROM delta.`{delta_path}/Locations` ORDER BY LocationID;")

# COMMAND ----------

scriptline_df = spark.sql(f'''
SELECT 
id as ScriptLineID, 
episode_id as EpisodeID, 
number as LineNumber, 
raw_text as Text, 
timestamp_in_ms as TimeStampMilliseconds,
CAST(speaking_line as BOOLEAN) as IsSpeakingLine,
CAST(character_id as INT) as CharacterID, 
CAST(location_id as INT) as LocationID,
spoken_words as SpokenWords,
CAST(IFNULL(word_count,0) as INT) as WordCount
FROM delta.`{delta_path}/ScriptLines` 
ORDER BY ScriptLineID
''')

# COMMAND ----------

# Write out to Delta Tables

def write_simpsons_df_to_delta(df, delta_path, partition_by=None):
    if partition_by is None:
        df.write.format("delta").save(delta_path)
    else:
        df.write.format("delta").partitionBy(partition_by).save(delta_path)

# COMMAND ----------

write_simpsons_df_to_delta(character_df, f"{output_path}/DimCharacter")
write_simpsons_df_to_delta(episode_df, f"{output_path}/FactEpisode",partition_by="season")
write_simpsons_df_to_delta(location_df, f"{output_path}/DimLocation")
write_simpsons_df_to_delta(scriptline_df, f"{output_path}/FactScriptLine")
