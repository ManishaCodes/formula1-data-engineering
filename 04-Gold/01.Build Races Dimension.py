# Databricks notebook source
# MAGIC %md
# MAGIC # Build Races Dimension
# MAGIC
# MAGIC 1. Read silver races table
# MAGIC 2. Read silver circuits table
# MAGIC 3. Join the data from races with circuits using circuit_id
# MAGIC 4. Select the required columns
# MAGIC    ○ races.season
# MAGIC    ○ races.round
# MAGIC    ○ races.race_name
# MAGIC    ○ races.race_date
# MAGIC    ○ circuits.circuit_name
# MAGIC    ○ circuits.locality
# MAGIC    ○ circuits.country
# MAGIC 5. Write the transformed data to gold dim_races table

# COMMAND ----------

# MAGIC %run "../00-common/01.environment-config"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {catalog_name}.{gold_schema}"))

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.dim_races"

# COMMAND ----------

circuits_df = spark.table(f"{catalog_name}.{silver_schema}.circuits")
races_df = spark.table(f"{catalog_name}.{silver_schema}.races")

# COMMAND ----------

dim_races_df = (
    races_df
    .join(
        circuits_df,
        races_df.circuit_id == circuits_df.circuit_id,
        "inner"
    )
    .select(
        races_df.season,
        races_df.round,
        races_df.race_name,
        races_df.race_date,
        circuits_df.circuit_name,
        circuits_df.locality,
        circuits_df.country
    )
)

# COMMAND ----------

display(dim_races_df)

# COMMAND ----------

(
    dim_races_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(target_table)
)


# COMMAND ----------

display(spark.table(target_table))