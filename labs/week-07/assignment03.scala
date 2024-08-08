package main.scala.chapter3
import org.apache.spark.sql.SparkSession 
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object assignment03{
        def main(args : Array[String]){
            val spark = SparkSession
                        .builder
                        .appName("assignment03")
                        .getOrCreate()

        //val Schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"
        val Schema = StructType(Array(StructField("date",StringType, false),
                                     StructField("delay",IntegerType, false),
                                     StructField("distance",IntegerType, false),
                                     StructField("origin",StringType, false),
                                     StructField("destination",StringType, false)))
        val file="/home/vagrant/learningSparkV2/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"
        
        val DataF = spark.read.schema(Schema)
                    .option("header", true)
                    .csv(file)

        DataF.show(false)
        println(DataF.printSchema)
            
        val DataF1 = DataF.withColumn("dateMonth", from_unixtime(unix_timestamp(DataF("date"), "MMddHHmm"), "MM")).withColumn("dateDay", from_unixtime(unix_timestamp(DataF("date"), "MMddHHmm"), "dd"))

        DataF1.show(false)
        println(DataF1.printSchema)

        DataF1.createOrReplaceTempView("Display1")

        //assignment part First
        val value1 = spark.sql("""SELECT max(delay), dateMonth FROM Display1 Group By dateMonth""")
        value1.show(10,false)
        val value2 = spark.sql("""SELECT max(delay), dateDay FROM Display1 Group By dateDay""")
        value2.show(10,false)

        //Dataframe
        val value3 = DataF1.groupBy("dateMonth").max("delay")    
        value3.show(10,false)

        val value4 = DataF1.groupBy("dateDay").max("delay")
        value4.show(10,false)

        println("---------------------------delay in flight is because of winter season--------------------------")
        
        //assignment part two 
        spark.sql("CREATE DATABASE assignment03")
        spark.sql("Use assignment03")
        val DataF2 = spark.read.schema(Schema).option("header",true).csv(file)
        val DataF3 = DataF2.withColumn("dateMonth", from_unixtime(unix_timestamp(DataF2("date"), "MMddHHmm"), "MM")).withColumn("dateDay", from_unixtime(unix_timestamp(DataF2("date"), "MMddHHmm"), "dd"))

        DataF3.write.mode("Overwrite").saveAsTable("us_delay_flights_tb1")
        DataF3.createOrReplaceTempView("Tempview")

        val value5 = spark.sql("""SELECT origin, dateMonth, dateDay, date
        FROM Tempview
        WHERE origin = 'ORD' AND dateMonth = 3 AND dateDay > 0 AND dateDay < 16
        ORDER BY dateDay""")
        
        value5.show(5,false)

        value5.createOrReplaceTempView("Tempview1")
        print(value5.show())

        println("Catalog")
        println(spark.catalog.listTables())

        //assignment part 3  add schema while reading
        // val Schema_all = "date String, delay INT, distance INT, origin STRING, destination STRING"
        val Schema_all =   StructType(Array(StructField("date",DateType, false),
                                     StructField("delay",IntegerType, false),
                                     StructField("distance",IntegerType, false),
                                     StructField("origin",StringType, false),
                                     StructField("destination",StringType, false)))
        
        //json
        val Json_read = spark.read.schema(Schema_all).option("header" ,true).csv(file)
        println(Json_read.printSchema())
        Json_read.show(false)
        val Json_write=DataF1.write.format("json").mode("overwrite").save("./departuredelays/json")
           
        //json using lz4
        val Snappyjson_write=DataF1.write.format("json").mode("overwrite").option("compression", "lz4").save("./departuredelays/lz4")

        //parquet
        val Parquet_write=DataF1.write.format("parquet").mode("overwrite").save("./departuredelays/parquet")


        //assignment part 4 
        val Parquet_file1    = "/home/vagrant/spatil62/itmd-521/labs/week-07/departuredelays/parquet/"
               //val Parquet_read = spark.read.format("parquet").load(Parquet_file)
        val Parquet_read1 = spark.read.schema(Schema).option("header",true).parquet(Parquet_file1)
        println(Parquet_read1.printSchema())
              //Parquet_read.show(false)
        
        Parquet_read1.createOrReplaceTempView("View2")
        val value6 = spark.sql("""SELECT date, delay, distance, origin, destination FROM View2 where origin = 'ORD'""")
        value6.show(10,false)
        
        val Parquet_write1 = Parquet_read1.write.format("parquet").mode("overwrite").save("./departuredelays/orddeparturedelays")

        }
}
