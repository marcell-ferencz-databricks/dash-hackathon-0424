# Databricks notebook source
# MAGIC %md
# MAGIC # Setup

# COMMAND ----------

dbutils.widgets.dropdown("Sample_Type", "sample_small", ["sample_small", "sample", "test", "all"])

# COMMAND ----------

spark.conf.set("spark.databricks.optimizer.adaptive.enabled", "false")

# COMMAND ----------

import mosaic as mos
import pyspark.sql.functions as F
import os
mos.enable_mosaic(spark, dbutils)
mos.enable_gdal(spark)

# COMMAND ----------

# DBTITLE 1,CHANGE THIS to your schema
# MAGIC %sql
# MAGIC USE CATALOG marcell;
# MAGIC USE geospatial;

# COMMAND ----------

RESOLUTION = 7

# COMMAND ----------

sample = dbutils.widgets.getArgument("Sample_Type")

# COMMAND ----------

# MAGIC %md
# MAGIC # Soil

# COMMAND ----------

features_df = spark.read.table(f"targets_{sample}")\
  .select("cell_id", "target", "drainagecl", "corsteel", "corcon", "slope_l", "slope_h", "slope_r")

# COMMAND ----------

features_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Weather Data

# COMMAND ----------

weather_df = spark.read.table("weather")

# COMMAND ----------

features_df = features_df.join(
  weather_df,
  on="cell_id",
  how="left"
)

# COMMAND ----------

features_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Hydrography Data

# COMMAND ----------

nhda_area_df = spark.read.table("hydorgraphy_area")\
  .withColumn("grid", mos.grid_tessellateexplode(mos.st_asbinary(mos.st_buffer(mos.st_geomfromwkt("geom_0"), F.lit(0))), F.lit(RESOLUTION)))\
  .drop("geom_0")\
  .withColumn("geom", F.col("grid.wkb"))\
  .where(mos.st_isvalid("geom"))\
  .withColumn("cell_id", F.col("grid.index_id"))

# COMMAND ----------

nhda_area_df = nhda_area_df.groupBy("cell_id").agg(
  F.mean("visibility").alias("visibility"),
  F.mean("elevation").alias("elevation")
)

# COMMAND ----------

features_df = features_df.join(
  nhda_area_df,
  on="cell_id",
  how="left"
)

# COMMAND ----------

features_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Roads

# COMMAND ----------

roads_df = spark.read.table("roads")

# ...


# COMMAND ----------

# roads_df = roads_df\
#   .withColumn("road_length", mos.st_length("geom"))

# COMMAND ----------

features_df.display()

# COMMAND ----------

features_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("features")
