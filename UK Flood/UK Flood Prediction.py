# Databricks notebook source
# spark.conf.set("spark.databricks.labs.mosaic.index.system", "BNG")

# COMMAND ----------

import mosaic as mos
import pyspark.sql.functions as F
import os
mos.enable_mosaic(spark, dbutils)
mos.enable_gdal(spark)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG marcell;
# MAGIC USE geospatial;

# COMMAND ----------

RESOLUTION = 6

# COMMAND ----------

# MAGIC %md
# MAGIC # Load Flood warning geoJSON

# COMMAND ----------

path = "dbfs:/FileStore/marcellferencz/flood_risk/uk_flood/Flood_Warning_Areas.json"

# COMMAND ----------

df_flood_warning = spark.read.format("ogr")\
    .option("driverName", "GeoJSON")\
    .option("asWKB", "true")\
    .load(path)

df_flood_warning.display()

# COMMAND ----------

df_flood_warning = df_flood_warning.withColumn("grid", mos.grid_tessellateexplode("geom_0", F.lit(RESOLUTION)))

# COMMAND ----------

df_flood_warning = df_flood_warning\
  .where(mos.st_isvalid("geom_0"))\
  .select(
    F.col("grid.index_id").alias("index_id"),
    F.col("grid.wkb").alias("wkb"),
    F.col("qdial")
  )

# COMMAND ----------

df_flood_warning.display()

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC df_flood_warning "wkb" "geometry"

# COMMAND ----------

# MAGIC %md
# MAGIC # Load Historical Floods

# COMMAND ----------

path = "dbfs:/FileStore/marcellferencz/flood_risk/uk_flood/Historic_Flood_Map.json"

# COMMAND ----------

df_historical_floods = spark.read.format("ogr")\
    .option("driverName", "GeoJSON")\
    .option("asWKB", "true")\
    .option("layer", "Historic Flood Map")\
    .load(path)

df_historical_floods.display()

# COMMAND ----------

df_historical_floods = df_historical_floods.withColumn("grid", mos.grid_tessellateexplode("geom_0", F.lit(RESOLUTION)))

# COMMAND ----------

df_historical_floods = df_historical_floods\
  .where(mos.st_isvalid("geom_0"))\
  .select(
    F.col("grid.index_id").alias("index_id"),
    F.col("grid.wkb").alias("wkb")
  )

# COMMAND ----------

df_historical_floods.display()

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC df_historical_floods "wkb" "geometry"

# COMMAND ----------

# MAGIC %md
# MAGIC # Agricultural Land Classification

# COMMAND ----------

path = "dbfs:/FileStore/marcellferencz/flood_risk/uk_flood/Agricultural_Land_Classification_Provisional_England.json"

# COMMAND ----------

df_agr_land_cls = spark.read.format("ogr")\
    .option("driverName", "GeoJSON")\
    .option("asWKB", "true")\
    .load(path)

df_agr_land_cls.display()

# COMMAND ----------

df_agr_land_cls = df_agr_land_cls.withColumn("grid", mos.grid_tessellateexplode("geom_0", F.lit(RESOLUTION)))

# COMMAND ----------

df_agr_land_cls = df_agr_land_cls\
  .where(mos.st_isvalid("geom_0"))\
  .select(
    F.col("grid.index_id").alias("index_id"),
    F.col("grid.wkb").alias("wkb"),
    F.col("alc_grade")
  )

# COMMAND ----------

df_agr_land_cls.display()

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC df_agr_land_cls "wkb" "geometry"

# COMMAND ----------

# MAGIC %md
# MAGIC # Join Datasets

# COMMAND ----------

df_flood_warning = df_flood_warning\
  .withColumn("Flood_warning", F.lit(1))\
  .select("index_id", "Flood_warning")

# COMMAND ----------

df_historical_floods = df_historical_floods\
  .withColumn("Historical_floods", F.lit(1))\
  .select("index_id", "Historical_floods")

# COMMAND ----------

df_agr_land_cls_piv = df_agr_land_cls\
  .groupBy("index_id").pivot("alc_grade").count()

# COMMAND ----------

df_features = df_flood_warning.join(
  df_historical_floods,
  on="index_id",
  how="fullouter"
).join(
  df_agr_land_cls_piv,
  on="index_id",
  how="fullouter"
).fillna(0)

# COMMAND ----------

df_features.display()

# COMMAND ----------

df_features.groupBy("Flood_warning").count().display()

# COMMAND ----------


