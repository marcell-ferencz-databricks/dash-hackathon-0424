# Databricks notebook source
# MAGIC %md
# MAGIC # Setup

# COMMAND ----------

import mosaic as mos
import pyspark.sql.functions as F
import os
mos.enable_mosaic(spark, dbutils)
mos.enable_gdal(spark)

# COMMAND ----------

# DBTITLE 1,CHANGE THIS to your schema
# MAGIC %sql
# MAGIC USE geospatial;

# COMMAND ----------

RESOLUTION = 8

# COMMAND ----------

# MAGIC %md
# MAGIC # Soil

# COMMAND ----------

# MAGIC %md
# MAGIC ## MUPolygon

# COMMAND ----------

mupolygon_df = spark.read.table("mupolygon").drop("Shape")

# COMMAND ----------

mupolygon_df_tesselated = (
  mupolygon_df
    # .repartition(200)
    .where(mos.st_isvalid("geom"))
    .withColumn("grid", mos.grid_tessellateexplode("geom", F.lit(RESOLUTION)))
    .withColumn("cell_id", F.col("grid.index_id"))
    .select("cell_id", "MUKEY")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabular: Component & Comonth

# COMMAND ----------

component_df = spark.read.table("component_train")

# COMMAND ----------

comonth_df = spark.read.table("comonth")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Combine all soil

# COMMAND ----------

soil_df = \
  comonth_df.join(component_df, on=["cokey"])\
  .join(mupolygon_df_tesselated, on=["mukey"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create target var

# COMMAND ----------

features_df = (
  soil_df
    .select("monthseq", "cell_id", "flodfreqcl", "drainagecl", "comppct_l", "comppct_r", "comppct_h", "slope_l", "slope_r", "slope_h", "airtempa_l", "airtempa_r", "airtempa_h")
    .where(F.col("flodfreqcl").isNotNull and F.col("flodfreqcl") != "None")
    .withColumn(
      "target", 
       F.when(F.col("flodfreqcl") == "Very frequent", 5)\
        .when(F.col("flodfreqcl") == "Frequent", 4)\
        .when(F.col("flodfreqcl") == "Occasional", 3)\
        .when(F.col("flodfreqcl") == "Rare", 2)\
        .when(F.col("flodfreqcl") == "Very rare", 1)\
        .otherwise(0)
    )
)

# COMMAND ----------

features_df = features_df.groupBy("cell_id").agg(
  F.mean("target").alias("target"),
  F.mean("comppct_l").alias("comppct_l"),
  F.mean("comppct_r").alias("comppct_r"),
  F.mean("comppct_h").alias("comppct_h"),
  F.mean("slope_l").alias("slope_l"),
  F.mean("slope_r").alias("slope_r"),
  F.mean("slope_h").alias("slope_h"),
  F.mean("airtempa_l").alias("airtempa_l"),
  F.mean("airtempa_r").alias("airtempa_r"),
  F.mean("airtempa_h").alias("airtempa_h")
  )

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

nhda_area_df.display()

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC nhda_area_df "geom" "geometry"

# COMMAND ----------

nhda_area_df = nhda_area_df.groupBy("cell_id").agg(
  F.mean("visibility").alias("visibility"),
  F.mean("elevation").alias("elevation")
)

# COMMAND ----------

features_df = features_df.join(
  nhda_area_df,
  on="cell_id"
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Roads

# COMMAND ----------

roads_df = spark.read.table("roads")\
  .withColumn("grid", mos.grid_tessellateexplode("geom_0", F.lit(RESOLUTION)))\
  .drop("geom_0")\
  .withColumn("geom", F.col("grid.wkb"))\
  .where(mos.st_isvalid("geom"))\
  .withColumn("cell_id", F.col("grid.index_id"))


# COMMAND ----------

roads_df = roads_df\
  .withColumn("road_length", mos.st_length("geom"))

# COMMAND ----------

roads_df = roads_df.select("cell_id", "road_length")

# COMMAND ----------

features_df = features_df.join(
  roads_df,
  on="cell_id",
  how="left"
)

features_df = features_df.groupby("cell_id").agg(
  F.mean("target").alias("target"),
  F.mean("comppct_l").alias("comppct_l"),
  F.mean("comppct_r").alias("comppct_r"),
  F.mean("comppct_h").alias("comppct_h"),
  F.mean("slope_l").alias("slope_l"),
  F.mean("slope_r").alias("slope_r"),
  F.mean("slope_h").alias("slope_h"),
  F.mean("airtempa_l").alias("airtempa_l"),
  F.mean("airtempa_r").alias("airtempa_r"),
  F.mean("airtempa_h").alias("airtempa_h"),
  F.mean("road_length").alias("road_length"),
  F.mean("weather_band_0").alias("weather_band_0"),
  F.mean("weather_band_1").alias("weather_band_1"),
  F.mean("weather_band_2").alias("weather_band_2"),
  F.mean("weather_band_3").alias("weather_band_3"),
  F.mean("weather_band_4").alias("weather_band_4"),
  F.mean("weather_band_5").alias("weather_band_5"),
  F.mean("weather_band_6").alias("weather_band_6"),
  F.mean("weather_band_7").alias("weather_band_7"),
  F.mean("weather_band_8").alias("weather_band_8"),
  F.mean("weather_band_9").alias("weather_band_9")
)

# COMMAND ----------

features_df.display()

# COMMAND ----------

features_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("features_train")

# COMMAND ----------


