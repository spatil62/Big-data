package main.scala.chapter3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object week05 {
    def main(args : Array[String]){

        val spark = SparkSession
        .builder
        .appName("week-05")
        .getOrCreate
   
    val file = "./Divvy_Trips_2015-Q1.csv"
    val dataFrame = spark.read.csv(file)
    
    dataFrame.show(false)
    println(dataFrame.printSchema)
    println(dataFrame.count())
    
    // schematically

    val dSchema = StructType(Array(StructField("trip_id", IntegerType, false),
    StructField("starttime", StringType, false),
    StructField("stoptime", StringType, false),
    StructField("bikeid", IntegerType, false),
    StructField("tripduration", IntegerType, false),
    StructField("from_station_id", IntegerType, false),
    StructField("from_station_name", StringType, false),
    StructField("to_station_id", IntegerType, false),
    StructField("to_station_name", StringType, false),
    StructField("usertype", StringType, false),
    StructField("gender", StringType, false),
    StructField("birthyear", IntegerType, false)))

    val dDataFrame = spark.read.schema(dSchema)
    .csv(file)

    dDataFrame.show(false)
    println(dDataFrame.printSchema)
    println(dDataFrame.count())

    //DLL

    val ddlSchema = "trip_id INT, starttime STRING, stoptime STRING, BikeID INT, TripDuration INT, from_station_id INT, from_station_name STRING, to_station_id INT, to_station_name STRING, usertype STRING, gender STRING, birthyear INT"

    val DataFrameDdl = spark.read.schema(ddlSchema)
    .csv(file)

    DataFrameDdl.show(false)
    println(DataFrameDdl.printSchema)
    println(DataFrameDdl.count())

    }
}

