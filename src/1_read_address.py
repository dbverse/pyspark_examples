import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SampleSparkCode") \
    .getOrCreate()

df_address = spark.read.csv("data\Address.csv",header=True)
df_address.show(5)
spark.stop()
