# Databricks notebook source
# MAGIC %md
# MAGIC # Setup

# COMMAND ----------

import os
import pyspark.sql.functions as F

# COMMAND ----------

ROOT_PATH = "/dbfs/FileStore/marcellferencz/flood_risk"
os.environ["ROOT_PATH"] = ROOT_PATH

ROOT_PATH_SPARK = "dbfs:/FileStore/marcellferencz/flood_risk"
os.environ["ROOT_PATH_SPARK"] = ROOT_PATH_SPARK

TEMP_PATH = "/databricks/driver/temp_geospatial"
os.environ["TEMP_PATH"] = TEMP_PATH

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir -p $TEMP_PATH
# MAGIC mkdir -p $ROOT_PATH

# COMMAND ----------

# MAGIC %md
# MAGIC # Soil Data
# MAGIC
# MAGIC https://www.nrcs.usda.gov/conservation-basics/natural-resource-concerns/soils/soil-geography

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir -p $ROOT_PATH/soil/data
# MAGIC wget -O $ROOT_PATH/soil/soil.zip https://dashhackdata.blob.core.windows.net/sampledata/geospatial/ny_soil/gSSURGO_NY.gdb.zip?sp=r&st=2024-04-09T09:49:42Z&se=2024-05-03T17:49:42Z&spr=https&sv=2022-11-02&sr=b&sig=WVoKf2DTj1z287nrpMy3qeHPiNLjHovU0VaATDLZ768%3D

# COMMAND ----------

# MAGIC %sh unzip $ROOT_PATH/soil/soil.zip -d $ROOT_PATH/soil/data

# COMMAND ----------

# MAGIC %md
# MAGIC # National Land Cover Data
# MAGIC
# MAGIC https://www.mrlc.gov/data?f%5B0%5D=category%3Aland%20cover&f%5B1%5D=region%3Aconus

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir -p $ROOT_PATH/vegetation

# COMMAND ----------

# MAGIC %sh wget 
# MAGIC rm -f $TEMP_PATH/nlcd_land_cover_2019.zip
# MAGIC wget -O $TEMP_PATH/nlcd_land_cover_2019.zip "https://s3-us-west-2.amazonaws.com/mrlc/nlcd_2019_land_cover_l48_20210604.zip"
# MAGIC cp $TEMP_PATH/nlcd_land_cover_2019.zip $ROOT_PATH/vegetation

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir -p $ROOT_PATH/vegetation/data
# MAGIC unzip $ROOT_PATH/vegetation/nlcd_land_cover_2019.zip -d $ROOT_PATH/vegetation/data

# COMMAND ----------

display(dbutils.fs.ls(f"{ROOT_PATH_SPARK}/vegetation/data"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Weather
# MAGIC
# MAGIC https://psl.noaa.gov/data/gridded/data.unified.daily.conus.html

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir -p $ROOT_PATH/weather/data

# COMMAND ----------

# MAGIC %sh 
# MAGIC rm -f $TEMP_PATH/precip.V1.0.day.ltm.1991-2020.nc
# MAGIC cd $TEMP_PATH && wget precip.V1.0.day.ltm.1991-2020.nc "https://downloads.psl.noaa.gov/Datasets/cpc_us_precip/precip.V1.0.day.ltm.1991-2020.nc" && cd ..
# MAGIC cp $TEMP_PATH/precip.V1.0.day.ltm.1991-2020.nc $ROOT_PATH/weather/data

# COMMAND ----------

display(dbutils.fs.ls(f"{ROOT_PATH_SPARK}/weather/data"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Hydrography
# MAGIC https://www.sciencebase.gov/catalog/item/61f8b8a5d34e622189c328b2

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir -p $ROOT_PATH/hydrography

# COMMAND ----------

# MAGIC %sh 
# MAGIC rm -f $TEMP_PATH/ny_hydrography.shp.zip
# MAGIC wget -O $TEMP_PATH/ny_hydrography.shp.zip "https://prd-tnm.s3.amazonaws.com/StagedProducts/Hydrography/NHD/State/Shape/NHD_H_New_York_State_Shape.zip"
# MAGIC cp $TEMP_PATH/ny_hydrography.shp.zip $ROOT_PATH/hydrography/

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir -p $ROOT_PATH/hydrography/data
# MAGIC unzip $ROOT_PATH/hydrography/ny_hydrography.shp.zip -d $ROOT_PATH/hydrography/data

# COMMAND ----------

display(dbutils.fs.ls(f"{ROOT_PATH_SPARK}/hydrography/data/Shape"))

# COMMAND ----------

# MAGIC %sh (cd $ROOT_PATH/hydrography/data/Shape/ && ls *.shp | sed 's/\.shp$//') > $TEMP_PATH/hydrography_shapes.txt

# COMMAND ----------

# MAGIC
# MAGIC %sh
# MAGIC
# MAGIC rm -rf $ROOT_PATH/hydgrography/shapefile
# MAGIC mkdir -p $ROOT_PATH/hydrography/shapefile
# MAGIC
# MAGIC rm -rf $TEMP_PATH/hydrography
# MAGIC mkdir $TEMP_PATH/hydrography
# MAGIC
# MAGIC FILES=$(cat $TEMP_PATH/hydrography_shapes.txt)
# MAGIC for f in $FILES
# MAGIC do
# MAGIC zip $TEMP_PATH/hydrography/$f.shp.zip $(ls $ROOT_PATH/hydrography/data/Shape/$f.*)
# MAGIC echo "copying $f.shp.zip..."
# MAGIC cp $TEMP_PATH/hydrography/$f.shp.zip $ROOT_PATH/hydrography/shapefile
# MAGIC done

# COMMAND ----------

display(dbutils.fs.ls(f"{ROOT_PATH_SPARK}/hydrography/shapefile"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Roads
# MAGIC https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html

# COMMAND ----------

# MAGIC %sh mkdir -p $ROOT_PATH/road/shapefile

# COMMAND ----------

# MAGIC %sh 
# MAGIC rm -f $TEMP_PATH/roads.txt
# MAGIC wget -O $TEMP_PATH/roads.txt "https://www2.census.gov/geo/tiger/TIGER_RD18/LAYER/ROADS/"
# MAGIC cp $TEMP_PATH/roads.txt $ROOT_PATH/road

# COMMAND ----------

tbl_start_row = (
  spark.read.text(f"{ROOT_PATH_SPARK}/road/roads.txt")
  .withColumn("row_num", F.monotonically_increasing_id())
  .withColumn("tbl_start_row", F.trim("value") == '<table>')
  .filter("tbl_start_row = True")
  .select("row_num")
).collect()[0][0]

tbl_end_row = (
  spark.read.text(f"{ROOT_PATH_SPARK}/road/roads.txt")
  .withColumn("row_num", F.monotonically_increasing_id())
  .withColumn("tbl_end_row", F.trim("value") == '</table>')
  .filter("tbl_end_row = True")
  .select("row_num")
).collect()[0][0]

print(f"tbl_start_row: {tbl_start_row}, tbl_end_row: {tbl_end_row}")

# COMMAND ----------

# new york is 36

ny_files = [r[1] for r in (
  spark.read.text(f"{ROOT_PATH_SPARK}/road/roads.txt")
  .withColumn("row_num", F.monotonically_increasing_id())
    .filter(f"row_num > {tbl_start_row}")
    .filter(f"row_num < {tbl_end_row}")
  .withColumn("href_start", F.substring_index("value", 'href="', -1))
  .withColumn("href", F.substring_index("href_start", '">', 1))
    .filter(F.col("href").startswith("tl_rd22_36"))
  .select("row_num","href")
).collect()]

print(f"len ny_files? {len(ny_files):,}")
ny_files[:5]

# COMMAND ----------

import pathlib
import requests

fuse_path = pathlib.Path(f"{ROOT_PATH}/road/shapefile")
fuse_path.mkdir(parents=True, exist_ok=True)

for idx,f in enumerate(ny_files):
  idx_str = str(idx).rjust(4)
  fuse_file = fuse_path / f 
  if not fuse_file.exists():
    print(f"{idx_str} --> '{f}'")
    req = requests.get(f'https://www2.census.gov/geo/tiger/TIGER_RD18/LAYER/ROADS/{f}')
    with open(fuse_file, 'wb') as f:
      f.write(req.content)
  else:
    print(f"{idx_str} --> '{f}' exists...skipping")

# COMMAND ----------

display(dbutils.fs.ls(f"{ROOT_PATH_SPARK}/road/shapefile"))

# COMMAND ----------


