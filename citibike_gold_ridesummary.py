# Databricks notebook source
from pyspark.sql.functions import *
saved_silver=spark.table("citi_bike_catalog.silvercitibike.silvercitibike")
saved_silver.printSchema()
rides_by_member=saved_silver.groupby("member_casual").agg(count("*").alias("count"))
display(rides_by_member)
rides_by_member.write.format("delta").mode("overwrite").saveAsTable("citi_bike_catalog.goldcitibike.rides_by_member")
display(spark.table("citi_bike_catalog.goldcitibike.rides_by_member"))

# COMMAND ----------

gold_data = spark.table("citi_bike_catalog.goldcitibike.rides_by_member")
display(gold_data)

# COMMAND ----------

from pyspark.sql.functions import count

rides_by_bike = saved_silver.groupBy("rideable").agg(count("*").alias("total_rides"))

rides_by_bike.write.format("delta").mode("overwrite").saveAsTable("citi_bike_catalog.goldcitibike.rides_by_bike")

# COMMAND ----------

from pyspark.sql.functions import avg, round

avg_duration = saved_silver.groupBy("member_casual").agg(round(avg("ride_duration_min"), 2).alias("avg_ride_duration_min"))

# COMMAND ----------

avg_duration.write.format("delta").mode("overwrite").saveAsTable("citi_bike_catalog.goldcitibike.avg_ride_duration")

# COMMAND ----------

rides_by_day=saved_silver.withColumn("day_of_week",date_format("started_at","EEEE")).groupBy("day_of_week",dayofweek("started_at").alias("day_number")).agg(count("*").alias("total_rides")).orderBy("day_number").drop("day_number")
rides_by_day.write.format("delta").mode("overwrite").saveAsTable("citi_bike_catalog.goldcitibike.rides_by_day")

# COMMAND ----------

rides_by_month=saved_silver.withColumn("month",date_format("started_at","yyyy-MM")).groupBy("month").agg(count("*").alias("total_rides")).orderBy("month")
rides_by_month.write.format("delta").mode("overwrite").saveAsTable("citi_bike_catalog.goldcitibike.rides_by_month")
