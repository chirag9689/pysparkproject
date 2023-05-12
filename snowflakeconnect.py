from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sfOptions = {
  "sfURL" : "knvvylq-ld21107.snowflakecomputing.com",
  "sfUser" : "MESHRAMCHIRAG",
    "sfPassword": "@Mumma143",
  "sfDatabase" : "sfspark",
  "sfSchema" : "public",
  "sfWarehouse" : "DEMO_WH"
}

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

df = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
    .options(**sfOptions) \
    .option("query", "select * from asl") \
    .load()
#https://mvnrepository.com/artifact/net.snowflake/spark-snowflake_2.12/2.11.0-spark_3.1
#https://mvnrepository.com/artifact/net.snowflake/snowflake-jdbc
#place these dependencies in spark/jars folders
opt = {
  "header" : "true",
  "inferSchema" : "true",
    "sep" : ";"
}
data="C:\\bigdata\\drivers\\bank-full.csv"
ndf=spark.read.format("csv").options(**opt).load(data)
ndf.show()
ndf.write.mode("append").format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable","banktab").save()