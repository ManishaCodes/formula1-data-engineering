# Databricks notebook source
# MAGIC %md
# MAGIC Ingest circuits.csv file
# MAGIC
# MAGIC 1. Read the file using spark dataframe reader API
# MAGIC 2. Add Metadata Columns
# MAGIC    ○ Source File
# MAGIC    ○ Ingestion Timestamp
# MAGIC 3. Write to bronze delta table

# COMMAND ----------

# MAGIC %md
# MAGIC step 01 - read the csv file using the dataframe reader api

# COMMAND ----------

# MAGIC %run "../00-common/01.environment-config"

# COMMAND ----------

# MAGIC %run "../00-common/02.bronze-helpers"

# COMMAND ----------

catalog_name

# COMMAND ----------

landing_folder_path

# COMMAND ----------

source_file = f"{landing_folder_path}/circuits.csv"
table_name = f"{catalog_name}.{bronze_schema}.circuits"

# COMMAND ----------

table_name

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType

circuits_schema = StructType([
    StructField("circuitId", StringType()),
    StructField("url", StringType()),
    StructField("circuitName", StringType()),
    StructField("lat", DoubleType()),
    StructField("long", DoubleType()),
    StructField("locality", StringType()),
    StructField("country", StringType())
])

# COMMAND ----------

circuits_df = (
    spark.read
        .format('csv')
        .option('header', 'true')
     #   .option('inferschema','true')
        .schema(circuits_schema)
        .load(source_file))

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 02. Add Metadata Columns ○ Source File ○ Ingestion Timestamp

# COMMAND ----------



circuits_final_df = add_ingestion_metadata(circuits_df)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC step 3 -Write to bronze delta table

# COMMAND ----------

(
    circuits_final_df
        .write
        .format('delta')
        .mode('overwrite')
        .saveAsTable(table_name)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1.bronze.circuits;

# COMMAND ----------

display(spark.table('formula1.bronze.circuits'))