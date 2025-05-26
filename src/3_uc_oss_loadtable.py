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

csv_path = "/home/obiwan/dbverse_git/pyspark_examples/data/Address.csv"
delta_table_path = "/home/obiwan/dbverse_git/pyspark_examples/src/processed/uc_address/"
try:
    df = spark.read.csv(
        csv_path,
        inferSchema=True,
        header=True
    )
    print(f"\nCSV data loaded from: {csv_path}")
    df.show(2)
except Exception as e:
    print(f"Error loading CSV data: {e}")
    spark.stop()
    exit()

try:

    df.write \
        .format("delta") \
        .option("overwriteSchema", "true") \
        .mode("overwrite") \
        .option("path",delta_table_path) \
        .save()
except Exception as e:
    print("This likely indicates a problem with Write.")
    spark.stop()

# ddl_statement = f"""
#     CREATE OR REPLACE TABLE demo.uc_emp.address
#     USING DELTA
#     LOCATION '{delta_table_path}'
#     COMMENT 'External table registered in Unity Catalog from existing Delta files';
#     """
# spark.sql(ddl_statement).show()

ddl_statement = f"""
    CREATE OR REPLACE TABLE demo.employee.address
    (AddressID INT, AddressLine1 STRING, AddressLine2 STRING, City STRING, StateProvince STRING, 
    CountryRegion STRING, PostalCode STRING, rowguid STRING, ModifiedDate TIMESTAMP)
    USING DELTA
    LOCATION '{delta_table_path}'
    COMMENT 'External table registered in Unity Catalog from existing Delta files';
    """
# spark.sql(ddl_statement).show()
spark.sql(ddl_statement)

spark.stop()