from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
host="jdbc:sqlserver://sateesh.c07uo2a23um4.ap-south-1.rds.amazonaws.com:1433;databaseName=dec"
user="dbuser"
pasw="mspassword"
tab="EMP"
msdri="com.microsoft.sqlserver.jdbc.SQLServerDriver"
#data importing /Extraction
#df=spark.read.format("jdbc").option("url",host).option("user",user).option("password",pasw).option("dbtable",tab).option("driver",msdri).load()
#df=spark.read.format("jdbc").option("url",host).option("user",user).option("password",pasw).option("query","select * from EMP where sal>2000").option("driver",msdri).load()
df=spark.read.format("jdbc").option("url",host).option("user",user).option("password",pasw).option("dbtable","(select * from EMP where sal<2000) abcd").option("driver",msdri).load()
#df.show()
#data processing /Transform

res=df.withColumn("HIREDATE",date_format(col("HIREDATE"),"dd-MMM-yyyy-EEE"))\
    .withColumn("fullsal",col("sal")+col("comm")).withColumn("today",current_date())
res.show()
#store / Load data
op="E:\\bigdata\\datasets\\output\\mssqldata"
#res.write.format("csv").option("header","true").save(op)
res.write.mode("append").format("jdbc").option("url",host).option("user",user)\
    .option("password",pasw).option("dbtable","dec2nd").option("driver",msdri).save()
'''SQL Server (Microsoft driver)	com.microsoft.sqlserver.jdbc.SQLServerDriver
Oracle	oracle.jdbc.OracleDriver
MariaDB	org.mariadb.jdbc.Driver
MySQL	com.mysql.jdbc.Driver
'''
# java.lang.ClassNotFoundException: com.microsoft.sqlserver.jdbc.SQLServerDriver
#if u get this error its dependency issue .. add mssql.jar to spark/jar foldder