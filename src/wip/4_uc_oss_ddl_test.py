from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time # Import time for potential delays

# Set up SparkSession for Unity Catalog OSS (UCSingleCatalog)
spark = SparkSession.builder \
    .appName("UC + Delta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "io.unitycatalog.spark.UCSingleCatalog") \
    .config("spark.sql.catalog.demo", "io.unitycatalog.spark.UCSingleCatalog") \
    .config("spark.sql.catalog.demo.uri", "http://localhost:8080") \
    .config("spark.sql.catalog.demo.token", "") \
    .config("spark.sql.defaultCatalog", "demo") \
    .getOrCreate()

spark.sql("show schemas").show()

spark.sql("use test").show()

# spark.sql("""
# create table demo_table (id int, desc string) using delta 
#           location "/home/obiwan/dbverse_git/pyspark_examples/src/processed/uc_address";
# """).show()

spark.sql("""
create table if not exists uc_address (
        AddressID INT,
        AddressLine1 STRING,
        AddressLine2 STRING,
        City STRING,
        StateProvince STRING,
        CountryRegion STRING,
        PostalCode STRING,
        rowguid STRING,
        ModifiedDate TIMESTAMP
    ) using delta 
          location "/home/obiwan/dbverse_git/pyspark_examples/src/processed/uc_address";
""").show()

spark.sql("describe demo.test.uc_address")

spark.stop()