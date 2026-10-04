# Databricks notebook source
from pyspark.sql.functions import count, col
saved_silver=spark.table("citi_bike_catalog.silvercitibike.silvercitibike")
saved_silver.printSchema()

rides_by_station = saved_silver.filter(col("start_station_name").isNotNull()).groupBy("start_station_name").agg(count("*").alias("total_rides")).orderBy(col("total_rides").desc())
rides_by_station.write.format("delta").mode("overwrite").saveAsTable("citi_bike_catalog.goldcitibike.rides_by_station")