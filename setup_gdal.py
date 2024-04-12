# Databricks notebook source
# MAGIC %pip install databricks-mosaic --quiet

# COMMAND ----------

import mosaic as mos

mos.enable_mosaic(spark, dbutils)
mos.setup_gdal(
  with_ubuntugis=False,
  with_mosaic_pip=True
)
