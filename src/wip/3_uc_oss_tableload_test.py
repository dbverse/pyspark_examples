# from pyspark.sql import SparkSession
# from pyspark.sql.functions import *


# # Set up SparkSession for Unity Catalog OSS (UCSingleCatalog)
# # spark = SparkSession.builder \
# #     .appName("UnityCatalogExample") \
# #     .config("spark.sql.catalog.demo", "io.unitycatalog.spark.UCSingleCatalog") \
# #     .config("spark.sql.catalog.demo.uri", "http://localhost:8080") \
# #     .config("spark.sql.catalog.demo.token", "") \
# #     .config("spark.sql.defaultCatalog", "demo") \
# #     .getOrCreate()

# spark = SparkSession.builder \
#   .appName("UC + Delta") \
#   .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
#   .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
#   .config("spark.sql.catalog.demo", "io.unitycatalog.spark.UCSingleCatalog") \
#   .config("spark.sql.catalog.demo.uri", "http://localhost:8080") \
#   .config("spark.sql.catalog.demo.token", "") \
#   .config("spark.sql.defaultCatalog", "demo") \
#   .getOrCreate()


# # Load the CSV data
# df = spark.read.csv(
#     "/home/obiwan/dbverse_git/pyspark_examples/data/Address.csv",
#     inferSchema=True,
#     header=True
# )

# df.show(2)

# # Save as Unity Catalog table (Delta format recommended for UC)
# df.write \
#   .format("delta") \
#   .option("overwriteSchema", "true") \
#   .mode("overwrite") \
#   .saveAsTable("demo.test.sample_address")

# #.option("path", "/home/obiwan/dbverse_git/pyspark_examples/src/processed/uc_address") \


# spark.stop()
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

print("SparkSession created successfully.")
print(f"Default Catalog: {spark.conf.get('spark.sql.defaultCatalog')}")

# Load the CSV data
csv_path = "/home/obiwan/dbverse_git/pyspark_examples/data/Address.csv"
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

# Define the target table name
catalog_name = "demo"
schema_name = "test"
table_name = "uc_address"
full_table_path = f"{catalog_name}.{schema_name}.{table_name}"

# Ensure the schema exists (optional but good practice for UC)
try:

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}").show()
    print(f"Schema '{catalog_name}.{schema_name}' ensured to exist.")
except Exception as e:
    print(f"Warning: Could not create/ensure schema '{catalog_name}.{schema_name}': {e}")
    print("This might indicate an issue with the Unity Catalog server or permissions.")


# Save as Unity Catalog table (Delta format recommended for UC)
print(f"\nAttempting to save DataFrame to table: {full_table_path}")
try:
    spark.sql(f"show schemas in demo").show()
    # spark.sql(f"USE  demo").show()
    # spark.sql(f"USE demo.test").show()
    df.write \
        .option("overwriteSchema", "true") \
        .mode("overwrite") \
        .saveAsTable("test.uc_address")
    print(f"Table '{full_table_path}' saved successfully (from Spark's perspective).")
except Exception as e:
    print(f"Error saving table '{full_table_path}': {e}")
    print("This likely indicates a problem with the connection to, or configuration of, your UCSingleCatalog server.")
    spark.stop()
    exit()

# --- Diagnostic Steps to Verify Catalog Metadata ---

print("\n--- Verifying Unity Catalog Metadata (from Spark's perspective) ---")

# 1. List Catalogs
print("\n1. Listing Catalogs:")
try:
    spark.sql("SHOW CATALOGS").show(truncate=False)
except Exception as e:
    print(f"Error listing catalogs: {e}")

# 2. List Schemas in 'demo' Catalog
print(f"\n2. Listing Schemas in '{catalog_name}' Catalog:")
try:
    # Set the current catalog to 'demo' for easier schema listing
    spark.sql(f"USE {catalog_name}").show()
    spark.sql(f"SHOW SCHEMAS IN {catalog_name}").show(truncate=False)
except Exception as e:
    print(f"Error listing schemas in {catalog_name}: {e}")

# 3. List Tables in 'demo.test' Schema
print(f"\n3. Listing Tables in '{full_table_path.rsplit('.', 1)[0]}' Schema:")
try:
    # Set the current schema to 'demo.test'
    spark.sql(f"USE {catalog_name}.{schema_name}").show()
    spark.sql(f"SHOW TABLES IN {catalog_name}.{schema_name}").show(truncate=False)
    # Also try describing the table directly
    print(f"\nAttempting to describe table '{full_table_path}':")
    spark.sql(f"DESCRIBE TABLE {full_table_path}").show(truncate=False)

except Exception as e:
    print(f"Error listing tables in {catalog_name}.{schema_name} or describing {full_table_path}: {e}")
    print("If you see 'Table or view not found', it means the table was not registered with the Unity Catalog server.")

# Small delay to ensure any background operations in UC server have a moment
time.sleep(5)

print("\nSpark application finished.")
spark.stop()
