# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Constructors Data
# MAGIC
# MAGIC 1. Read bronze constructors table
# MAGIC 2. Keep only the columns required for analytics (Drop url column)
# MAGIC 3. Standardise column names using snake_case (constructorId → constructor_id)
# MAGIC 4. Rename columns to make them more meaningful (name → constructor_name)
# MAGIC 5. Remove duplicate records
# MAGIC 6. Transform values of column nationality to Title Case
# MAGIC 7. Write the transformed data to silver constructors table

# COMMAND ----------

# MAGIC %run "../00-common/01.environment-config"

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.constructors"
silver_table = f"{catalog_name}.{silver_schema}.constructors"

# COMMAND ----------

constructors_df = spark.table(bronze_table)

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

constructors_dropped_df = constructors_df.drop("url")

# COMMAND ----------

constructors_renamed_df = (
    constructors_dropped_df
        .withColumnsRenamed({
            "constructorId": "constructor_id",
            "name": "constructor_name"
        })
)

# COMMAND ----------

display(constructors_renamed_df)

# COMMAND ----------

constructors_distinct_df = constructors_renamed_df.dropDuplicates(["constructor_id"])

# COMMAND ----------

display(constructors_distinct_df)

# COMMAND ----------

constructors_final_df = (
    constructors_distinct_df
        .withColumn("nationality", F.initcap(F.col("nationality")))
)

# COMMAND ----------

display(constructors_final_df)

# COMMAND ----------

(
    constructors_final_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(silver_table)
)

# COMMAND ----------

display(spark.table(silver_table))