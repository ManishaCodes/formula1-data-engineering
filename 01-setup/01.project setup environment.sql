-- Databricks notebook source
-- MAGIC %md
-- MAGIC # set up the project environment for formula 1 project

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1.create external location databricks-course-ext-d1-formula1
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # access cloud storage

-- COMMAND ----------

-- MAGIC %fs ls 'abfss://formula1@databrickscourseextd1.dfs.core.windows.net/landing'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # create external location

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS demo_ext_dl_formula1
URL 'abfss://formula1@databrickscourseextd1.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `databricks-course-sc`)
COMMENT 'External location for the formula1 container';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. create unity catalog formula1
-- MAGIC

-- COMMAND ----------

SHOW CATALOGS;

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS formula1
MANAGED LOCATION 'abfss://formula1@databrickscourseextd1.dfs.core.windows.net/'
COMMENT 'This is the main catalog for the formula1 project';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS formula1.landing;
CREATE SCHEMA IF NOT EXISTS formula1.bronze
    MANAGED LOCATION 'abfss://formula1@databrickscourseextd1.dfs.core.windows.net/bronze';
CREATE SCHEMA IF NOT EXISTS formula1.silver
    MANAGED LOCATION 'abfss://formula1@databrickscourseextd1.dfs.core.windows.net/silver';
CREATE SCHEMA IF NOT EXISTS formula1.gold
    MANAGED LOCATION 'abfss://formula1@databrickscourseextd1.dfs.core.windows.net/gold';

-- COMMAND ----------

select current_catalog();

-- COMMAND ----------

use catalog formula1;

-- COMMAND ----------

show schemas;

-- COMMAND ----------

CREATE EXTERNAL VOLUME formula1.landing.files
LOCATION 'abfss://formula1@databrickscourseextd1.dfs.core.windows.net/landing';

-- COMMAND ----------

-- MAGIC %fs ls /Volumes/formula1/landing/files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC