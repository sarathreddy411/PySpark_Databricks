# Databricks notebook source
bronze_data=spark.sql("select * from citi_bike_catalog.bronzecitibike.bronzecitibike")
silver_data=bronze_data.dropDuplicates(["ride_id"])
# display(silver_data)
silver_data.write.format("delta").mode("overwrite").option("mergeSchema","true").saveAsTable("citi_bike_catalog.silvercitibike.silvercitibiketable")

# COMMAND ----------

from pyspark.sql.functions import count, col
sil=silver_data.groupBy("rideable").agg(count("*").alias("count"))
sil.filter(col("count") > 1).show()

# COMMAND ----------

from pyspark.sql.functions import expr

null_counts = silver_data.select(
    *[
        expr(f"sum(case when `{c}` is null then 1 else 0 end)").alias(c)
        for c in silver_data.columns
    ]
)

display(null_counts)

# COMMAND ----------

silver_data=silver_data.filter(col("ride_id").isNotNull())
silver_data.write.format("delta").mode("overwrite").option("mergeSchema","true").saveAsTable("citi_bike_catalog.silvercitibike.silvercitibiketable")

# COMMAND ----------


from pyspark.sql.functions import col, round
silver_data=silver_data.withColumn("ride_duration_min",round((col("started_at").cast("long")-col("ended_at").cast("long"))/60,2))
silver_data.write.format("delta").mode("overwrite").option("mergeSchema","true").saveAsTable("citi_bike_catalog.silvercitibike.silvercitibike")

# COMMAND ----------

saved_silver=spark.table("citi_bike_catalog.silvercitibike.silvercitibike")
saved_silver.printSchema()

# COMMAND ----------

invalid_rides=saved_silver.filter((col("ride_duration_min")>0)|(col("ride_duration_min").isNull()))
display(invalid_rides.limit(10))

# COMMAND ----------

silver_data.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("citi_bike_catalog.silvercitibike.silvercitibike")