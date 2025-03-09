from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .appName("Spark Example")\
    .master("local")\
    .getOrCreate()

df = spark.createDataFrame([('tom', 20), ('jack', 40)], ['name', 'age'])

print(df.count())

spark.stop()