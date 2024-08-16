
from pyspark.sql import SparkSession 
from pyspark.sql.types import *
from pyspark.sql.functions import col
from pyspark.sql.functions import max
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import year
from pyspark.sql.functions import month
from pyspark.sql.functions import weekofyear
from pyspark.sql.functions import desc

spark = (SparkSession
.builder
.appName("assignment-02")
.getOrCreate())


fireSchema = StructType([StructField('CallNumber', IntegerType(), True),
 StructField('UnitID', StringType(), True),
 StructField('IncidentNumber', IntegerType(), True),
 StructField('CallType', StringType(), True), 
 StructField('CallDate', StringType(), True), 
 StructField('WatchDate', StringType(), True),
 StructField('CallFinalDisposition', StringType(), True),
 StructField('AvailableDtTm', StringType(), True),
 StructField('Address', StringType(), True), 
 StructField('City', StringType(), True), 
 StructField('Zipcode', IntegerType(), True), 
 StructField('Battalion', StringType(), True), 
 StructField('StationArea', StringType(), True), 
 StructField('Box', StringType(), True), 
 StructField('OriginalPriority', StringType(), True), 
 StructField('Priority', StringType(), True), 
 StructField('FinalPriority', IntegerType(), True), 
 StructField('ALSUnit', BooleanType(), True), 
 StructField('CallTypeGroup', StringType(), True),
 StructField('NumAlarms', IntegerType(), True),
 StructField('UnitType', StringType(), True),
 StructField('UnitSequenceInCallDispatch', IntegerType(), True),
 StructField('FirePreventionDistrict', StringType(), True),
 StructField('SupervisorDistrict', StringType(), True),
 StructField('Neighborhood', StringType(), True),
 StructField('Location', StringType(), True),
 StructField('RowID', StringType(), True),
 StructField('Delay', FloatType(), True)])

File="/home/vagrant/learningSparkV2/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv"

fireDF = spark.read.schema(fireSchema).option("header", "true").csv(File)
print(fireDF.show())
print(fireDF.printSchema())

result = fireDF.withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy")).drop("CallDate")
result.select(year("IncidentDate"))

 # What were all the different types of fire calls in 2018?
(result
 .select("CallType")
 .where((year("IncidentDate"))=="2018")
 .groupby("CallType")
 .count()
 .show(truncate = False))

 # What months within the year 2018 saw the highest number of fire calls?
(result
 .select("IncidentDate")
 .where((year("IncidentDate"))=="2018")
 .groupby(month("IncidentDate"))
 .count()
 .orderBy('count',ascending=False)
 .show())

# Which neighborhood in San Francisco generated the most fire calls in 2018?
(result
.select("Neighborhood")
.where(((year("IncidentDate")) == "2018") & (col("City") == "San Francisco"))
.groupby(("Neighborhood"))
.count()
.show(30,truncate = False))

# Which neighborhoods had the worst response times to fire calls in 2018?
(result
.select("Neighborhood", "Delay")
.where(((year("IncidentDate")) == "2018") & (col("City") == "San Francisco"))
.sort(col("Delay").desc())
.show(5,truncate=False))

# Which week in the year in 2018 had the most fire calls?
(result
 .select("IncidentDate")
 .where((year("IncidentDate"))=="2018")
 .groupby(weekofyear("IncidentDate"))
 .count()
 .show(40))

# How can we use Parquet files or SQL tables to store this data and read it back?
#parquet_path = "./"
parquet_path = "~"
result.write.format("parquet").save(parquet_path)

# Is there a correlation between neighborhood, zip code, and number of fire calls?
(fireDF
 .select("Neighborhood","Zipcode")
 .stat.corr("Neighborhood","Zipcode")
 .count()
 .orderBy('count')
 .show())




















