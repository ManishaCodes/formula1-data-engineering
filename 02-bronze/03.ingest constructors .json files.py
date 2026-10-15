# Databricks notebook source
# MAGIC %run "../00-common/01.environment-config"

# COMMAND ----------

# MAGIC %run "../00-common/02.bronze-helpers"

# COMMAND ----------

source_file = f"{landing_folder_path}/constructors.json"
table_name = f"{catalog_name}.{bronze_schema}.constructors"

# COMMAND ----------

# Define the schema
constructors_schema = """constructorId STRING,
                         name STRING,
                         nationality STRING,
                         url STRING"""

# COMMAND ----------

# Read data from constructors file
constructors_df = (
    spark.read
        .format('json')
        .schema(constructors_schema)
        .option('mode', 'FAILFAST')
        .load(source_file)
)

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

constructors_final_df = add_ingestion_metadata(constructors_df)

# COMMAND ----------

(
    constructors_final_df
        .write
        .format('delta')
        .mode('overwrite')
        .saveAsTable(table_name)
)

# COMMAND ----------

display(spark.table(table_name))