package main.scala.chapter3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object assignment02 {
    def main(args : Array[String]){
        val spark = (SparkSession
                .builder
                .appName("assignment02")
                .getOrCreate())
 

    val fireSchema = StructType(Array(StructField("device_id", IntegerType, false),
    StructField("device_name", StringType, false),
    StructField("ip", StringType, false),
    StructField("cca2", StringType, false),
    StructField("cca3", StringType, false), 
    StructField("cn", StringType, false),
    StructField("latitude", StringType, false),
    StructField("longitude", DoubleType, false),
    StructField("scale", StringType, false),
    StructField("temp", IntegerType, false), 
    StructField("humidity", IntegerType, false), 
    StructField("battery_level", IntegerType, false), 
    StructField("c02_level", IntegerType, false), 
    StructField("lcd", StringType, false), 
    StructField("timestamp", LongType, false)))    

    val File_Path="/home/vagrant/learningSparkV2/databricks-datasets/learning-spark-v2/iot-devices/iot_devices.json"

    val DataF = spark.read.schema(fireSchema)
        .json(File_Path)

    DataF.show(false)
    println(DataF.printSchema)

    DataF.createOrReplaceTempView("framedf")

    //Detect failing devices with battery levels below a threshold.
    val First = spark.sql("Select device_name,device_id,battery_level from framedf where battery_level < 10")
    First.show(20, false)

    //Identify offending countries with high levels of C02 emissions.
    val Second = spark.sql("Select cca3,c02_level from framedf where c02_level = 1599")
    Second.show(20, false)

    //Compute the min and max values for temperature, battery level, C02, and humidity
    val Third = spark.sql("Select min(temp),min(battery_level),min(c02_level),min(humidity),max(temp),max(battery_level),max(c02_level),max(humidity) from framedf")
    Third.show(20, false)

    //Sort and group by average temperature, Co2, humidity, and country
    val Four = spark.sql("Select AVG(temp),AVG(c02_level),AVG (humidity),cca3 from framedf group by cca3")
    Four.show(20, false)
    
    }
}