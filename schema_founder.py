from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .appName("Schema_founder")\
    .master("local")\
    .getOrCreate()

df_yellow = spark.read.csv("dataset/yellow_csv/yellow_tripdata_2009-01.csv")

df_yellow.printSchema()

df_green = spark.read.parquet("dataset/green/green_tripdata_2014-01.csv")

df_green.printSchema()

df_fhv = spark.read.parquet("dataset/fhv_tripdata_2024-01.csv")

df_fhv.printSchema()

df_fhvhv = spark.read.parquet("dataset/fhvhv_tripdata_2024-01.csv")

df_fhvhv.printSchema()

spark.stop()