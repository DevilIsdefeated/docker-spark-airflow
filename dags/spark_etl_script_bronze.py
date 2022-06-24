import requests
import json
from pyspark.sql import SparkSession
from pyspark import SQLContext
from pyspark.sql import functions as F
from decouple import config
from datetime import date

today = date.today().strftime("%b-%d-%Y")

aws_access_key = config('AWS_ACCESS_KEY')
aws_secret_key = config('AWS_SECRET_KEY')

spark = SparkSession \
    .builder \
    .appName("DataExtraction") \
    .getOrCreate() 

hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", aws_access_key)
hadoop_conf.set("fs.s3a.secret.key", aws_secret_key)
hadoop_conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

response = requests.get("https://api.mfapi.in/mf/118550")
data = response.text
sparkContext = spark.sparkContext
RDD = sparkContext.parallelize([data])
raw_json_dataframe = spark.read.json(RDD)

raw_json_dataframe.printSchema()
raw_json_dataframe.createOrReplaceTempView("Mutual_benefit")

dataframe = raw_json_dataframe.withColumn("data", F.explode(F.col("data"))) \
        .withColumn('meta', F.expr("meta")) \
        .select("data.*", "meta.*")
        
dataframe.show(100, False)
dataframe.toPandas().to_csv("dataframe.csv")

tables_names = ['Part_in_Order', 'Supplier', 'Brand', 'Part', 'Part_for_Car', 'Part_Supplier', \
               'Customer', 'Customer_Statut', 'Orders', 'Car_Manufacturer', 'Car', 'Part_Maker']

for table_name in tables_names:
    dataframe.write.format('parquet').save(f's3a://new-wave-delta-lake/bronze/CarPartsDB/{today}/{table_name}', mode='overwrite')