# Databricks notebook source
# MAGIC %pip install scikit-learn

# COMMAND ----------

import mosaic as mos
import pyspark.sql.functions as F
import os
from sklearn.model_selection import train_test_split 
mos.enable_mosaic(spark, dbutils)
mos.enable_gdal(spark)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG marcell;
# MAGIC USE geospatial;

# COMMAND ----------

component_df = spark.read.table("component")
comonth_df = spark.read.table("comonth")

soil_df = comonth_df.join(component_df, on=["cokey"])\
  .select("cokey", "flodfreqcl")\
    .withColumn(
      "target", 
       F.when(F.col("flodfreqcl") == "Very frequent", 5)\
        .when(F.col("flodfreqcl") == "Frequent", 4)\
        .when(F.col("flodfreqcl") == "Occasional", 3)\
        .when(F.col("flodfreqcl") == "Rare", 2)\
        .when(F.col("flodfreqcl") == "Very rare", 1)\
        .otherwise(0)
    )\
      .groupBy("cokey").agg(F.mean("target").alias("target"))

df_soil = soil_df.toPandas()

# COMMAND ----------

df_soil.target = df_soil.target.map(lambda x: int(x))

df_soil.target.value_counts()

# COMMAND ----------

df_train, df_test = train_test_split(df_soil, stratify=df_soil["target"], test_size=0.2)

# COMMAND ----------

df_train.target.value_counts()

# COMMAND ----------

df_test.target.value_counts()

# COMMAND ----------

soil_train_ids = spark.createDataFrame(df_train).select("cokey")
soil_test_ids = spark.createDataFrame(df_test).select("cokey")

# COMMAND ----------

component_df_train = component_df.join(soil_train_ids, on="cokey")
component_df_test = component_df.join(soil_test_ids, on="cokey")

# COMMAND ----------

component_df_train.count()

# COMMAND ----------

component_df_test.count()

# COMMAND ----------

component_df_train.write.format("delta").mode("overwrite").saveAsTable("component_train")
component_df_test.write.format("delta").mode("overwrite").saveAsTable("component_test")

# COMMAND ----------


