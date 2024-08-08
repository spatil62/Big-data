from pyspark.sql import SparkSession 
from pyspark.sql.types import *

spark = (SparkSession
.builder
.appName("week-05")
.getOrCreate())



dataframe = spark.read.csv("./Divvy_Trips_2015-Q1.csv")
print(dataframe.show())
print(dataframe.printSchema())
print(dataframe.count())

schema = StructType([StructField("trip_id",IntegerType(), False),
    StructField("starttime",StringType(), False),
    StructField("stoptime",StringType(), False),
    StructField("bikeid",IntegerType(), False),
    StructField("tripduration",IntegerType(), False),
    StructField("from_station_id",IntegerType(), False),
    StructField("from_station_name", StringType(), False),
    StructField("to_Station_id",IntegerType(), False),
    StructField("to_Station_name",StringType(), False),
    StructField("usertype",StringType(), False),
    StructField("gender",StringType(), False),
    StructField("birthyear",IntegerType(), False),
    ])

data = spark.read.csv("./Divvy_Trips_2015-Q1.csv",header = True, schema = schema)
print(data.show())
print(data.printSchema())
print(data.count())


# DLL 

ddl_schema = "trip_id INT, starttime STRING, stoptime STRING, BikeID INT, TripDuration INT, from_station_id INT, from_station_name STRING, to_station_id INT, to_station_name STRING, usertype STRING, gender STRING, birthyear INT"

ddl_display = spark.read.csv("./Divvy_Trips_2015-Q1.csv", header = True, schema = ddl_schema)
print(ddl_display.show())
print(ddl_display.printSchema())
print(ddl_display.count())
