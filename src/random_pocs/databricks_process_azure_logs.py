# THIS CODE WORKS JUST FINE - FLATTENS ALL STRUCT AND ARRAY PROPERLY IN DATABRICKS>
# LOCAL SPARK HAD LOT OF ISSUES
# Databricks notebook source

dbutils.fs.ls("dbfs:/FileStore")

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/FileStore/azure_logs")


# COMMAND ----------

# dbutils.fs.cp("dbfs:/FileStore/trigger_runs.json", "dbfs:/FileStore/azure_logs/trigger_runs.json")
# dbutils.fs.cp("dbfs:/FileStore/activity_runs.json", "dbfs:/FileStore/azure_logs/activity_runs.json")
# dbutils.fs.cp("dbfs:/FileStore/pipeline_runs.json", "dbfs:/FileStore/azure_logs/pipeline_runs.json")


# COMMAND ----------

file_list = dbutils.fs.ls("dbfs:/FileStore/azure_logs")
file_list = [file.path for file in file_list]
print(file_list)


# COMMAND ----------

from functools import reduce

# Initialize Spark session (if not already initialized)
# List of JSON file paths to read

# Step 1: Read all JSON files into Spark DataFrames
df_list = [spark.read.option("lines","true").json(file) for file in file_list]

# Step 2: Handle Schema Differences and Union All DataFrames
df_final = reduce(lambda x, y: x.unionByName(y, allowMissingColumns=True), df_list)

# Step 3: Show Final Merged DataFrame
display(df_final)

# COMMAND ----------

df_spark_final.printSchema

# COMMAND ----------

from pyspark.sql.functions import col, to_json, json_tuple, explode
from pyspark.sql.types import StructType, StructField, StringType
import pandas as pd
import json
import numpy as np


# Step 1: Convert ALL Struct Columns to JSON Before Moving to Pandas
# Identify all struct columns and convert them to JSON
struct_columns = [field.name for field in df_final.schema.fields if isinstance(field.dataType, StructType)]

df_json = df_final
for col_name in struct_columns:
    df_json = df_json.withColumn(col_name, to_json(col(col_name)))  # Convert struct to JSON string

# Convert Spark DataFrame to Pandas
pdf = df_json.toPandas()



# Function to Recursively Flatten JSON Columns
def flatten_json(nested_json, prefix=""):
    """Recursively flattens a JSON object into key-value pairs."""
    out = {}
    if isinstance(nested_json, dict):
        for key, value in nested_json.items():
            full_key = f"{prefix}_{key}" if prefix else key
            if isinstance(value, dict):
                out.update(flatten_json(value, full_key))  # Recursively flatten
            elif isinstance(value, list):  # Handle list (arrays)
                if all(isinstance(i, dict) for i in value):  # If array of structs
                    for i, item in enumerate(value):
                        out.update(flatten_json(item, f"{full_key}_{i}"))  # Flatten each item
                else:
                    out[full_key] = json.dumps(value) if value else None  # Convert to JSON string
            else:
                out[full_key] = value
    else:
        out[prefix] = nested_json
    return out

# Step 2: Fully Flatten ALL Struct Columns Dynamically
for col_name in struct_columns:
    pdf[col_name] = pdf[col_name].apply(lambda x: json.loads(x) if isinstance(x, str) else x)  # Convert JSON string back to dict
    pdf_expanded = pdf[col_name].apply(lambda x: flatten_json(x) if isinstance(x, dict) else {})
    pdf_expanded_df = pd.DataFrame(pdf_expanded.tolist())  # Convert dict to DataFrame
    pdf = pd.concat([pdf.drop(columns=[col_name]), pdf_expanded_df], axis=1)  # Merge flattened columns

# Ensure correct data types before converting to Spark
for col in pdf.columns:
    if pdf[col].dtype == 'object':
        pdf[col] = pdf[col].astype(str)

# Explicit schema definition
schema = StructType([StructField(col, StringType(), True) for col in pdf.columns])
pdf = pdf.replace({np.nan: None})
pdf = pdf.where(pd.notna(pdf), None)  # Extra check
# Convert Pandas DataFrame Back to PySpark
df_spark_final = spark.createDataFrame(pdf, schema=schema)
df_spark_final = df_spark_final.fillna("")

# Show the Final Fully Flattened DataFrame in Databricks
display(df_spark_final)


# COMMAND ----------

df_spark_final.createOrReplaceTempView("tempview1")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe tempview1

# COMMAND ----------

display(df_spark_final)
