# Databricks notebook source
from pyspark.sql.types import (
    StructType, StructField, StringType,
    TimestampType, DoubleType,DecimalType
)
citi_bikeschema=StructType([StructField('ride_id',StringType(),True),
                            StructField('rideable',StringType(),True),
                            StructField('started_at', TimestampType(), True),
                            StructField('ended_at', TimestampType(), True),
                            StructField('start_station_name', StringType(), True),
                            StructField('start_station_id', StringType(), True),
                            StructField('end_station_name', StringType(), True),
                            StructField('end_station_id', StringType(), True),
                            StructField('start_lat', DecimalType(10, 10), True),
                            StructField('start_lng', DecimalType(10, 10), True),
                            StructField('end_lat', DecimalType(10, 10), True),
                            StructField('end_lng', DecimalType(10, 10), True),
                            StructField('member_casual', StringType(), True)])
spark.sql("USE CATALOG citi_bike_catalog")
spark.sql("USE SCHEMA bronzecitibike")
data=spark.read.schema(citi_bikeschema).csv("/Volumes/citi_bike_catalog/citibikeschema/citi_bike_volume/",header=True,schema=citi_bikeschema)
data.write.format("delta").mode("overwrite").saveAsTable("bronzecitibike")
# data.printSchema()
# data.show()
spark.sql("SHOW TABLES IN citi_bike_catalog.bronzecitibike").show()
# print("Current catalog:")
# spark.sql("SELECT current_catalog()").show()

# print("Current schema:")
# spark.sql("SELECT current_schema()").show()
# spark.sql("USE CATALOG citi_bike_catalog")
# spark.sql("USE SCHEMA bronzecitibike")
# spark.sql("SHOW SCHEMAS IN citi_bike_catalog").show()
display(
    spark.table(
        "citi_bike_catalog.bronzecitibike.bronzecitibike"
    )
)