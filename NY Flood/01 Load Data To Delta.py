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

# MAGIC %sh gdalinfo --version

# COMMAND ----------

# DBTITLE 1,CHANGE THIS to your schema
# MAGIC %sql
# MAGIC USE geospatial;

# COMMAND ----------

# DBTITLE 1,CHANGE these to your paths
ROOT_PATH = "/dbfs/FileStore/marcellferencz/flood_risk"
os.environ["ROOT_PATH"] = ROOT_PATH

ROOT_PATH_SPARK = "dbfs:/FileStore/marcellferencz/flood_risk"
os.environ["ROOT_PATH_SPARK"] = ROOT_PATH_SPARK

RESOLUTION = 8

# COMMAND ----------

# MAGIC %md
# MAGIC # Roads

# COMMAND ----------

display(dbutils.fs.ls(f"{ROOT_PATH_SPARK}/road/shapefile/"))

# COMMAND ----------

roads_df = mos.read()\
  .format("multi_read_ogr")\
  .option("vsizip", "true")\
  .load(f"{ROOT_PATH_SPARK}/road/shapefile/")

# COMMAND ----------

roads_df.write\
  .format("delta")\
  .mode("overwrite")\
  .saveAsTable("roads")

# COMMAND ----------

# MAGIC %md
# MAGIC # Weather

# COMMAND ----------

display(dbutils.fs.ls(f"{ROOT_PATH_SPARK}/weather/data"))

# COMMAND ----------

#mos.rst_fromfile("path")

weather_metadata_df = (
  spark.read.format("gdal").option("pathGlobFilter", "*.nc").load(f"{ROOT_PATH_SPARK}/weather/data")
    .drop("content")
      .withColumn("tmp", F.col("subdatasets.precip"))
      # .withColumn("subdatasets_str", F.from_json("subdatasets"))
      .withColumn("precip_tile", mos.rst_getsubdataset(F.col("tile"), F.lit("precip")))
      .withColumn("num_bands", mos.rst_numbands("tile"))
      .withColumn("metadata2", mos.rst_metadata("tile"))
      .withColumn("summary", mos.rst_summary("tile"))
)

weather_metadata_df.display()

# COMMAND ----------

weather_df = mos.read()\
  .format("raster_to_grid")\
  .option("resolution", str(RESOLUTION))\
  .option("readSubdataset", "true")\
  .option("subdatasetName", "precip")\
  .option("combiner", "mean")\
  .load(f"{ROOT_PATH_SPARK}/weather/data")

weather_df.display()

# COMMAND ----------

pivoted_weather_df = weather_df.filter("band_id < 10").groupby("cell_id").pivot("band_id").mean("measure")

# COMMAND ----------

pivoted_weather_df = pivoted_weather_df.select([F.col(col).alias(f"weather_band_{col}") if not col=="cell_id" else F.col("cell_id") for col in pivoted_weather_df.columns])

# COMMAND ----------

pivoted_weather_df.write.format("delta").mode("overwrite").saveAsTable("weather")

# COMMAND ----------

# MAGIC %md
# MAGIC # Hydrography

# COMMAND ----------

display(dbutils.fs.ls(f"{ROOT_PATH_SPARK}/hydrography/data/Shape/"))

# COMMAND ----------

nhda_area = mos.read()\
  .format("multi_read_ogr")\
  .option("vsizip", "false")\
  .load(f"{ROOT_PATH_SPARK}/hydrography/data/Shape/NHDArea.shp")\

nhda_area.display()

# COMMAND ----------

nhda_area.write.format("delta").mode("overwrite").saveAsTable("hydorgraphy_area")

# COMMAND ----------

nhda_line = spark.read\
  .format("ogr")\
  .option("vsizip", "false")\
  .load(f"{ROOT_PATH_SPARK}/hydrography/data/Shape/NHDLine.shp")

nhda_line.display()

# COMMAND ----------

nhda_line.write.format("delta").mode("overwrite").saveAsTable("hydorgraphy_line")

# COMMAND ----------

# MAGIC %md
# MAGIC # Soil

# COMMAND ----------

# MAGIC %md
# MAGIC ## MUpolygon

# COMMAND ----------

mupolygon_df = mos.read()\
  .format("multi_read_ogr")\
  .option("vsizip", "true")\
  .option("layerName", "mupolygon")\
  .load(f"{ROOT_PATH_SPARK}/soil/")\
  .repartition(200, F.col("Shape"))\
  .withColumn("geom", mos.st_updatesrid("Shape", "Shape_srid", F.lit(4326)))

mupolygon_df.display()

# COMMAND ----------

mupolygon_df.write.format("delta").mode("overwrite").saveAsTable("mupolygon")

# COMMAND ----------

# MAGIC %md
# MAGIC ## SApolygon

# COMMAND ----------

sapolygon_df = mos.read()\
  .format("multi_read_ogr")\
  .option("vsizip", "true")\
  .option("layerName", "sapolygon")\
  .load(f"{ROOT_PATH_SPARK}/soil/")\
  .repartition(200, F.col("Shape"))\
  .withColumn("geom", mos.st_updatesrid("Shape", "Shape_srid", F.lit(4326)))#\
  # .select(mos.grid_tessellateexplode("geom", F.lit(RESOLUTION)).alias("grid"))\
  # .select(
  #   F.col("grid.is_core").alias("is_core")
  # )\
  # .repartition(200)


# .select(F.col("grid.index_id").alias("cell_id"))\

# COMMAND ----------

sapolygon_df.limit(10).display()

# COMMAND ----------

sapolygon_df.write.format("delta").mode("overwrite").saveAsTable("sapolygon")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Component

# COMMAND ----------

component_df = mos.read()\
  .format("multi_read_ogr")\
  .option("vsizip", "true")\
  .option("layerName", "component")\
  .load(f"{ROOT_PATH_SPARK}/soil/")\

component_df.display()

# COMMAND ----------

component_df.write.format("delta").mode("overwrite").saveAsTable("component")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comonth

# COMMAND ----------

comonth_df = mos.read()\
  .format("multi_read_ogr")\
  .option("vsizip", "true")\
  .option("layerName", "comonth")\
  .load(f"{ROOT_PATH_SPARK}/soil/")\

comonth_df.display()

# COMMAND ----------

comonth_df.write.format("delta").mode("overwrite").saveAsTable("comonth")
