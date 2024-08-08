from pyspark.sql import SparkSession 
from pyspark.sql.functions import col
from pyspark.sql.functions import *
from pyspark.sql.types import *


#from us_delay_flights_tbl import date 

spark = (SparkSession
 .builder
 .appName("assignment03")
 .getOrCreate())

file = "/home/vagrant/learningSparkV2/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"

#ddl_schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"
schema =  StructType([StructField("date",StringType(),False),
                             StructField("delay",IntegerType(),False),
                             StructField("distance",IntegerType(),False),
                             StructField("origin",StringType(),False),
                            StructField("destination",StringType(),False)])

ddl_display = spark.read.csv(file, header = True,  schema = schema)


print(ddl_display.show(10))
print(ddl_display.printSchema())


ddl_display = ddl_display.withColumn("dateMonth", from_unixtime(unix_timestamp(ddl_display.date, "MMddHHmm"), "MM")).withColumn("dateDay", from_unixtime(unix_timestamp(ddl_display.date, "MMddHHmm"), "dd"))

print(ddl_display.show(10))
print(ddl_display.printSchema())

ddl_display.createOrReplaceTempView("ddl_display_new")

#assignment part First
print("Spark sql")
spark.sql("""SELECT max(delay), dateMonth FROM ddl_display_new Group By dateMonth""").show(10)
spark.sql("""SELECT max(delay), dateDay FROM ddl_display_new Group By dateDay""").show(10)

#Dataframe
print("Dataframe API")
ddl_display.groupBy("dateMonth").max("delay").show(10)
ddl_display.groupBy("dateDay").max("delay").show(10)

print("------------------------------Delay in flight is because of winter season--------------------------")
        

#assignment part Second
print("Database create")

spark.sql("CREATE DATABASE assignment03")
spark.sql("Use assignment03")
ddl_display_new = spark.read.csv(file, header = True,  schema = schema)

ddl_display_new = ddl_display_new.withColumn("dateMonth", from_unixtime(unix_timestamp(ddl_display_new.date, "MMddHHmm"), "MM")).withColumn("dateDay", from_unixtime(unix_timestamp(ddl_display_new.date, "MMddHHmm"), "dd"))

ddl_display_new.write.mode("Overwrite").saveAsTable("us_delay_flights_tb1")
ddl_display_new.createOrReplaceTempView("Tempview")

print("verify")
print(ddl_display_new.show())

spark.sql("""SELECT origin, dateMonth, dateDay, date
             FROM Tempview
             WHERE origin = 'ORD' AND dateMonth = 3 AND dateDay > 0 AND dateDay < 16
             ORDER BY dateDay""").show(5)

value = spark.sql("""SELECT origin, dateMonth, dateDay, date
             FROM Tempview
             WHERE origin = 'ORD' AND dateMonth = 3 AND dateDay > 0 AND dateDay < 16
             ORDER BY dateDay""")

value.createOrReplaceTempView("Tempview1")
print(value.show(5))

print("Catalog")
print(spark.catalog.listTables())






#assignment part Third
schema_all = StructType([StructField("date",DateType(),False),
                             StructField("delay",IntegerType(),False),
                             StructField("distance",IntegerType(),False),
                             StructField("origin",StringType(),False),
                            StructField("destination",StringType(),False)])
#ddl_schema_all = "date DateType, delay INT, distance INT, origin STRING, destination STRING"                             
#JSON
Format = spark.read.csv(file, header = True,  schema = schema_all)
Format.printSchema()
Format.write.format("json").mode("overwrite").save("./departuredelays/json")

#json using lz4  
#json_snappyFormat = spark.read.csv(file, header = True,  schema = ddl_schema_all)
Format.write.format("json").mode("overwrite").option("compression", "lz4").save("./departuredelays/lz4")

#parquet
#parquet_Format = spark.read.csv(file, header = True,  schema = ddl_schema_all)
Format.write.format("parquet").mode("overwrite").save("./departuredelays/parquet")



#assignment part four
parquet_file = """/home/vagrant/spatil62/itmd-521/labs/week-07/departuredelays/parquet/"""
df = spark.read.format("parquet").load(parquet_file)
df.printSchema()

df.createOrReplaceTempView("View")

spark.sql("""SELECT date, delay, distance, origin, destination FROM View where origin = 'ORD'""").show(10)

#value_sql.write.format("parquet").mode("overwrite").save("./departuredelays/orddeparturedelays")



df.write.format("parquet").mode("overwrite").save("./departuredelays/orddeparturedelays")

