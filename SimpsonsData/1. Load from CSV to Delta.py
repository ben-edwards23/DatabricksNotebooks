# Databricks notebook source
# MAGIC %run "./HelperNotebooks/MountDataLakeStorage"

# COMMAND ----------

dbutils.widgets.text("FileLocation", "/mnt/datalake/Bronze/Simpsons/")
dbutils.widgets.text("FileType", "CSV")
dbutils.widgets.text("DeltaPath", "/mnt/datalake/Silver/Simpsons")

# COMMAND ----------

file_location = dbutils.widgets.get("FileLocation")
file_type = dbutils.widgets.get("FileType")
delta_path = dbutils.widgets.get("DeltaPath")

# COMMAND ----------

def read_simpsons_csv(filename):
    df = spark.read.format(file_type).option("inferSchema", True).option("header", True).option("sep", ",").load(f"{file_location}/{filename}.csv")
    return df

# COMMAND ----------

character_df = read_simpsons_csv("simpsons_characters")
episodes_df = read_simpsons_csv("simpsons_episodes")
locations_df = read_simpsons_csv("simpsons_locations")
scriptlines_df = read_simpsons_csv("simpsons_script_lines")

# COMMAND ----------

# Write out to Delta Tables

def write_simpsons_df_to_delta(df, delta_path, partition_by=None):
    if partition_by is None:
        df.write.format("delta").save(delta_path)
    else:
        df.write.format("delta").partitionBy(partition_by).save(delta_path)

# COMMAND ----------


write_simpsons_df_to_delta(character_df, f"{delta_path}/Characters")
write_simpsons_df_to_delta(episodes_df, f"{delta_path}/Episodes",partition_by="season")
write_simpsons_df_to_delta(locations_df, f"{delta_path}/Locations")
write_simpsons_df_to_delta(scriptlines_df, f"{delta_path}/ScriptLines")


# COMMAND ----------


