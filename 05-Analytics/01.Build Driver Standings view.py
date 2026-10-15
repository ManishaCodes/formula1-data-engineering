# Databricks notebook source
# MAGIC %md
# MAGIC # Build Driver Standings
# MAGIC
# MAGIC Sources
# MAGIC 1. fact_session_results
# MAGIC 2. dim_drivers
# MAGIC
# MAGIC Output Columns
# MAGIC 1. season
# MAGIC 2. driver id
# MAGIC 3. driver name
# MAGIC 4. nationality
# MAGIC 5. race starts
# MAGIC 6. total points
# MAGIC 7. number of wins
# MAGIC 8. number of podiums
# MAGIC 9. standing position

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW formula1.gold.v_driver_standing
# MAGIC AS
# MAGIC WITH driver_session_summary
# MAGIC as
# MAGIC (SELECT
# MAGIC     r.season,
# MAGIC     d.driver_id,
# MAGIC     d.driver_name,
# MAGIC     d.nationality,
# MAGIC     COUNT(*) AS race_starts,
# MAGIC     SUM(r.points) AS total_points,
# MAGIC     COUNT_IF(r.is_win) AS number_of_wins,
# MAGIC     COUNT_IF(r.is_podium) AS number_of_podiums
# MAGIC FROM formula1.gold.fact_session_results r
# MAGIC JOIN formula1.gold.dim_drivers d
# MAGIC     ON r.driver_id = d.driver_id
# MAGIC GROUP BY
# MAGIC     r.season,
# MAGIC     d.driver_id,
# MAGIC     d.driver_name,
# MAGIC     d.nationality)
# MAGIC SELECT
# MAGIC     RANK() OVER (PARTITION BY season ORDER BY total_points desc, number_of_wins desc ) AS standing,
# MAGIC     season,
# MAGIC     driver_id,
# MAGIC     driver_name,
# MAGIC     nationality,
# MAGIC     race_starts,
# MAGIC     total_points,
# MAGIC     number_of_wins,
# MAGIC     total_points
# MAGIC     number_of_podiums
# MAGIC FROM driver_session_summary;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM formula1.gold.v_driver_standing
# MAGIC WHERE season = 2025