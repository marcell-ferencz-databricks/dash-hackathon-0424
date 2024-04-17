# Databricks notebook source
# DBTITLE 1,Set Indexing System to British National Grid
spark.conf.set("spark.databricks.labs.mosaic.index.system", "BNG")
spark.conf.set("spark.databricks.optimizer.adaptive.enabled", "false")

# COMMAND ----------

# DBTITLE 1,Import Libraries
import mosaic as mos
import pyspark.sql.functions as F
import os
mos.enable_mosaic(spark, dbutils)
mos.enable_gdal(spark)

# COMMAND ----------

# DBTITLE 1,CHANGE THIS - set schema
# MAGIC %sql
# MAGIC USE geospatial;

# COMMAND ----------

# DBTITLE 1,Set resolution
RESOLUTION = 6

# COMMAND ----------

# DBTITLE 1,CHANGE THIS - change to your own directory where you downloaded the JSONs
root_path = "dbfs:/FileStore/marcellferencz/flood_risk/uk_flood"

# COMMAND ----------

# MAGIC %md
# MAGIC # Load Flood warning geoJSON
# MAGIC
# MAGIC Source: [Flood Warning Areas](https://environment.data.gov.uk/dataset/87e5d78f-d465-11e4-9343-f0def148f590)

# COMMAND ----------

# DBTITLE 1,Generate path
flood_warning_path = f"{root_path}/Flood_Warning_Areas.json"

# COMMAND ----------

# DBTITLE 1,Read in geoJSON
df_flood_warning = spark.read.format("ogr")\
    .option("driverName", "GeoJSON")\
    .option("asWKB", "true")\
    .load(flood_warning_path)

df_flood_warning.display()

# COMMAND ----------

# DBTITLE 1,Tessellate and explode
df_flood_warning = df_flood_warning.withColumn("grid", mos.grid_tessellateexplode("geom_0", F.lit(RESOLUTION)))

# COMMAND ----------

# DBTITLE 1,Select index, shape and a feature
df_flood_warning = df_flood_warning\
  .where(mos.st_isvalid("geom_0"))\
  .select(
    F.col("grid.index_id").alias("index_id"),
    F.col("grid.wkb").alias("wkb"),
    F.col("qdial")
  ).cache()

df_flood_warning.display()

# COMMAND ----------

# DBTITLE 1,Uncomment to plot with Kepler
# MAGIC %%mosaic_kepler
# MAGIC df_flood_warning "wkb" "geometry" 10000

# COMMAND ----------

# MAGIC %md
# MAGIC # Load Historical Floods
# MAGIC
# MAGIC Source: [Historic Flood Map](https://environment.data.gov.uk/dataset/889885c0-d465-11e4-9507-f0def148f590)

# COMMAND ----------

historical_floods_path = f"{root_path}/Historic_Flood_Map.json"

# COMMAND ----------

df_historical_floods = spark.read.format("ogr")\
    .option("driverName", "GeoJSON")\
    .option("asWKB", "true")\
    .option("layer", "Historic Flood Map")\
    .load(historical_floods_path)

# df_historical_floods.display()

# COMMAND ----------

df_historical_floods = df_historical_floods.withColumn("grid", mos.grid_tessellateexplode("geom_0", F.lit(RESOLUTION)))

# COMMAND ----------

df_historical_floods = df_historical_floods\
  .where(mos.st_isvalid("geom_0"))\
  .select(
    F.col("grid.index_id").alias("index_id"),
    F.col("grid.wkb").alias("wkb")
  )

# df_historical_floods.display()

# COMMAND ----------

# DBTITLE 1,Uncomment to Plot with Kepler
# %%mosaic_kepler
# df_historical_floods "wkb" "geometry"

# COMMAND ----------

# MAGIC %md
# MAGIC # Agricultural Land Classification
# MAGIC
# MAGIC Source: [Agricultural Land Classification](https://environment.data.gov.uk/dataset/af1b847b-037b-4772-9c31-7edf584522aa)

# COMMAND ----------

acl_path = f"{root_path}/Agricultural_Land_Classification_Provisional_England.json"

# COMMAND ----------

df_agr_land_cls = spark.read.format("ogr")\
    .option("driverName", "GeoJSON")\
    .option("asWKB", "true")\
    .load(acl_path)

# df_agr_land_cls.display()

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

# df_agr_land_cls.display()

# COMMAND ----------

# DBTITLE 1,Uncomment to plot with Kepler
# %%mosaic_kepler
# df_agr_land_cls "wkb" "geometry"

# COMMAND ----------

# MAGIC %md
# MAGIC # Join Datasets

# COMMAND ----------

# DBTITLE 1,Assign a 1 to areas with flood warning as a binary variable
df_flood_warning = df_flood_warning\
  .withColumn("Flood_warning", F.lit(1))\
  .select("index_id", "Flood_warning")

# COMMAND ----------

# DBTITLE 1,Assign a 1 to areas with historical flooding as a binary variable
df_historical_floods = df_historical_floods\
  .withColumn("Historical_floods", F.lit(1))\
  .select("index_id", "Historical_floods")

# COMMAND ----------

# DBTITLE 1,One-hot-encode ALC grades
df_agr_land_cls_piv = df_agr_land_cls\
  .groupBy("index_id").pivot("alc_grade").count()

# COMMAND ----------

# DBTITLE 1,Outer join everything on index
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

# DBTITLE 1,Rename column names to remove spaces
df_features = df_features\
  .select(
    *(F.col(col).alias(col.replace(" ", "_")) for col in df_features.columns)
  )

# COMMAND ----------

# df_features.display()

# COMMAND ----------

# DBTITLE 1,write to Delta
df_features.write.format("delta").mode("overwrite").saveAsTable("uk_flood_features")
