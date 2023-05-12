from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
import boto3
#s3=boto3.client("s3")
s3t=boto3.resource('s3')
#s3.create_bucket(Bucket='mybucket')

#s3t.create_bucket(Bucket='vaishnaviproject2020', CreateBucketConfiguration={'LocationConstraint': 'ap-south-1'})