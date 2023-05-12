from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
data="C:\\bigdata\\drivers\\us-500.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data).withColumnRenamed("zip","sal").select("first_name","state","sal")
#df.show()
#find second highest salary
#res=df.orderBy(col("sal").desc())
#res.show()
#usually partition column is category based column.

win =Window.partitionBy(col("state")).orderBy(col("sal").desc())
'''res=df.withColumn("rank",rank().over(win))\
    .withColumn("drank",dense_rank().over(win))\
    .withColumn("rno",row_number().over(win))\
    .withColumn("prank",percent_rank().over(win))\
    .withColumn("grade",ntile(4).over(win))\
    .withColumn("lead",lead(col("sal"),1).over(win))\
    .withColumn("lag",lag(col("sal"),1).over(win)).na.fill(0)\
    .withColumn("diff",col("sal")-col("lead"))\
    .withColumn("first", first(col("sal")).over(win)).withColumn("df",col("first")-col("sal"))\
'''
df.createOrReplaceTempView("tab")
res=spark.sql("select first_name, state, sal, dense_rank() over (partition by state order by sal desc) drank, rank() over (partition by state order by sal desc) rank, row_number() over (partition by state order by sal desc) rno from tab")

res.show(40)

#rank vs dense_rank()
#rank not in sequence manner ... rank let eg: 1,2,2,2,5,6 u ll get.. but in dense rank u ll get 1,2,2,2,3,4 like that rank must be in seq
#dense rank .. rank must be in sequence especially when u get duplicate values.
#row num.. not bother about duplicate values always give 1,2,3,4, .. there is no duplicate rank