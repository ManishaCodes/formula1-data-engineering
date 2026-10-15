# Databricks notebook source
# MAGIC %md
# MAGIC # Build Results Fact
# MAGIC
# MAGIC 1. Read silver results table
# MAGIC 2. Read silver sprints table
# MAGIC 3. Add new column session_type with values RACE or SPRINT
# MAGIC 4. UNION results and sprints
# MAGIC 5. Derive additional columns
# MAGIC    ○ is_win -> Indicates that the driver won the race
# MAGIC    ○ is_podium -> Indicates that the driver scored a podium result (1, 2, 3)
# MAGIC    ○ has_points -> Indicates that the driver has scored points
# MAGIC 6. Write the transformed data to gold fact_session_results table

# COMMAND ----------

# MAGIC %run "../00-common/01.environment-config"

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.fact_session_results"

# COMMAND ----------


from pyspark.sql import functions as F

# COMMAND ----------

results_df = (
    spark.table(f"{catalog_name}.{silver_schema}.results")
    .withColumn("session_type", F.lit("RACE"))
    .drop("race_name", "race_date", "ingestion_timestamp", "source_file")
)

# COMMAND ----------

sprints_df = (
    spark.table(f"{catalog_name}.{silver_schema}.sprints")
    .withColumn("session_type", F.lit("RACE"))
    .drop("race_name", "race_date", "ingestion_timestamp", "source_file")
)

# COMMAND ----------

results_sprints_df = results_df.unionByName(sprints_df)

# COMMAND ----------

fact_session_results_df = (
    results_sprints_df
    .withColumn("is_win", F.col("final_position") == 1)
    .withColumn("is_podium", F.col("final_position").between(1, 3))
    .withColumn("has_points", F.col("points") > 0)
)

# COMMAND ----------

display(results_sprints_df)

# COMMAND ----------

(
    fact_session_results_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(target_table)
)

# COMMAND ----------

display(spark.table(target_table))