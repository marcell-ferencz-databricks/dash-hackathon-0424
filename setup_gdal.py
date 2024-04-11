# Databricks notebook source
# MAGIC %pip install databricks-mosaic --quiet

# COMMAND ----------

import mosaic as mos

mos.enable_mosaic(spark, dbutils)
mos.setup_gdal(
  to_fuse_dir="/Workspace/Users/marcell.ferencz@databricks.com/DASH Hackathon/gdal",
  with_ubuntugis=False,
  with_mosaic_pip=True,
  override_mosaic_version="==0.4.0"
)
