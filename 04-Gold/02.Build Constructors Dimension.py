# Databricks notebook source
# MAGIC %md
# MAGIC # Build Constructors Dimension
# MAGIC Read silver constructors table
# MAGIC Read gold ref_nationality_region table
# MAGIC Join the data from constructors with ref_nationality_region using nationality
# MAGIC Select the required columns ○ constructors.constructor_id ○ constructors.constructor_name ○ constructors.nationality ○ ref_nationality_region.region
# MAGIC Write the transformed data to gold dim_constructors table

# COMMAND ----------

# MAGIC %run "../00-common/01.environment-config"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.dim_constructors"

# COMMAND ----------

constructors_df = spark.table(f"{catalog_name}.{silver_schema}.constructors")
ref_nationality_region_df = spark.table(f"{catalog_name}.{gold_schema}.ref_nationality_region")

# COMMAND ----------

dim_constructors_df = (
    constructors_df
    .join(
        ref_nationality_region_df,
        constructors_df.nationality == ref_nationality_region_df.nationality,
        "left"
    )
    .select(
        constructors_df.constructor_id,
        constructors_df.constructor_name,
        constructors_df.nationality,
        ref_nationality_region_df.region.alias("nationality_region")
    )
)

# COMMAND ----------

display(dim_constructors_df)

# COMMAND ----------

(
    dim_constructors_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(target_table)
)

# COMMAND ----------

display(spark.table(target_table))