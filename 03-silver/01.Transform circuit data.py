# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Transform Circuits Data
# MAGIC
# MAGIC 1. Read bronze circuits table
# MAGIC 2. Keep only the columns required for analytics (Drop url column)
# MAGIC 3. Standardise column names using snake_case (circuitId → circuit_id, circuitName → circuit_name)
# MAGIC 4. Rename columns to make them more meaningful (lat → latitude, long → longitude)
# MAGIC 5. Filter out rows where circuit_id is null (business key validation)
# MAGIC 6. Remove duplicate records
# MAGIC 7. Transform values of columns circuit_name and locality to Title Case
# MAGIC 8. Write the transformed data to silver circuits table

# COMMAND ----------

# MAGIC %run "../00-common/01.environment-config"

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.circuits"
silver_table = f"{catalog_name}.{silver_schema}.circuits"

# COMMAND ----------

# circuits_df = spark.read.table(bronze_table)

# COMMAND ----------

circuits_df = spark.table(bronze_table)

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

#circuits_selected_df = circuits_df.select(
   # "circuitId",
  #  "circuitName",
   # "lat",
   # "long",
   # "locality",
   # "country",
   # "ingestion_timestamp",
   ## "source_file"
#)

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

circuits_selected_df = circuits_df.select(
    F.col("circuitId"),
    F.col("circuitName"),
    F.col("lat"),
    F.col("long"),
    F.col("locality"),
    F.col("country"),
    F.col("ingestion_timestamp"),
    F.col("source_file")
)

# COMMAND ----------

circuits_renamed_df = (
    circuits_selected_df
        .withColumnsRenamed({
            "circuitId": "circuit_id",
            "circuitName": "circuit_name",
            "lat": "latitude",
            "long": "longitude"
        })
)

# COMMAND ----------

circuits_valid_df = circuits_renamed_df.filter(
    F.col("circuit_id").isNotNull()
)

# COMMAND ----------

display(circuits_valid_df)


# COMMAND ----------

# circuits_distinct_df = circuits_valid_df.distinct()

# COMMAND ----------

circuits_distinct_df = circuits_valid_df.dropDuplicates(["circuit_id"])

# COMMAND ----------

display(circuits_distinct_df)

# COMMAND ----------

circuits_final_df = (
    circuits_distinct_df
        .withColumn("circuit_name", F.initcap(F.col("circuit_name")))
        .withColumn("locality", F.initcap(F.col("locality")))
)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

(
    circuits_final_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(silver_table)
)

# COMMAND ----------

display(spark.table(silver_table))