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



spark.sql("CREATE DATABASE IF NOT EXISTS dwh COMMENT 'Data Warehouse for Car Part'")


# Reading tables from landing area
print('\nReading ...')
Brand = spark.read.format("parquet").load(f's3a://new-wave-delta-lake/bronze/CarPartsDB/{today}/Brand')
Car = spark.read.format("parquet").load(f's3a://new-wave-delta-lake/bronze/CarPartsDB/{today}/Car')
Customer = spark.read.format("parquet").load(f's3a://new-wave-delta-lake/bronze/CarPartsDB/{today}/Customer')
Orders = spark.read.format("parquet").load(f's3a://new-wave-delta-lake/bronze/CarPartsDB/{today}/Orders')
Part_for_Car = spark.read.format("parquet").load(f's3a://new-wave-delta-lake/bronze/CarPartsDB/{today}/Part_for_Car')
Part_in_Order = spark.read.format("parquet").load(f's3a://new-wave-delta-lake/bronze/CarPartsDB/{today}/Part_in_Order')
Part_Maker = spark.read.format("parquet").load(f's3a://new-wave-delta-lake/bronze/CarPartsDB/{today}/Part_Maker')
Part_Supplier = spark.read.format("parquet").load(f's3a://new-wave-delta-lake/bronze/CarPartsDB/{today}/Part_Supplier')
Part = spark.read.format("parquet").load(f's3a://new-wave-delta-lake/bronze/CarPartsDB/{today}/Part')
Supplier = spark.read.format("parquet").load(f's3a://new-wave-delta-lake/bronze/CarPartsDB/{today}/Supplier')
print('End of reading... \n')



# transforming tables to a set of dimensional tables
print('\ntransforming ...')
Brand.write.format('parquet').mode('overwrite').option('path','s3a://new-wave-delta-lake/silver/warehouse/CarParts/Dim_Brand').saveAsTable("dwh.DimBrand")
Car.write.format('parquet').mode('overwrite').option('path','s3a://new-wave-delta-lake/silver/warehouse/CarParts/Dim_Car').saveAsTable("dwh.DimCar")
Customer.write.format('parquet').mode('overwrite').option('path','s3a://new-wave-delta-lake/silver/warehouse/CarParts/Dim_Customer').saveAsTable("dwh.DimCustomer")
Orders.write.format('parquet').mode('overwrite').option('path','s3a://new-wave-delta-lake/silver/warehouse/CarParts/Dim_Orders').saveAsTable("dwh.DimOrders")
Part_Maker.write.format('parquet').mode('overwrite').option('path','s3a://new-wave-delta-lake/silver/warehouse/CarParts/Dim_Part_Maker').saveAsTable("dwh.DimPartMaker")
Part_for_Car.write.format('parquet').mode('overwrite').option('path','s3a://new-wave-delta-lake/silver/warehouse/CarParts/Dim_Part_for_Car').saveAsTable("dwh.DimPartForCar")
Part_Supplier.write.format('parquet').mode('overwrite').option('path','s3a://new-wave-delta-lake/silver/warehouse/CarParts/Dim_Part_Supplier').saveAsTable("dwh.DimPartSupplier")
Supplier.write.format('parquet').mode('overwrite').option('path','s3a://new-wave-delta-lake/silver/warehouse/CarParts/Dim_Supplier').saveAsTable("dwh.DimSupplier")
Part.write.format('parquet').mode('overwrite').option('path','s3a://new-wave-delta-lake/silver/warehouse/CarParts/Dim_Part').saveAsTable("dwh.DimPart")


#TODO Data has gotten to the Silver bed. Join all the data into one Warehouse, that will be the company's data.





