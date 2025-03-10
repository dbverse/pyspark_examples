# Databricks notebook source
from pyspark.sql.functions import col, explode, lit
from pyspark.sql.types import StructType, StructField


# COMMAND ----------

df1=spark.read.json("dbfs:/FileStore/samplelog2.json",multiLine=True)

# COMMAND ----------

display(df1)

# COMMAND ----------

df2=spark.read.json("dbfs:/FileStore/samplelog3.json",multiLine=True)

# COMMAND ----------



def flatten_properties(df):
    """
    Flattens only the `properties` column while leaving top-level fields unchanged.
    """
    top_level_columns = [field.name for field in df.schema.fields if field.name != "properties"]
    properties_columns = []

    # If `properties` is a struct, extract its fields and rename them
    if "properties" in df.columns and isinstance(df.schema["properties"].dataType, StructType):
        for field in df.schema["properties"].dataType.fields:
            properties_columns.append(col(f"properties.{field.name}").alias(f"properties_{field.name}"))

    # Select top-level fields + flattened properties fields
    return df.select(*top_level_columns, *properties_columns)

# Step 1: Flatten only the `properties` column
df1_flat = flatten_properties(df1)
df2_flat = flatten_properties(df2)

# Step 2: Ensure both DataFrames have the same schema by adding missing columns
all_columns = set(df1_flat.columns) | set(df2_flat.columns)

for col_name in all_columns:
    if col_name not in df1_flat.columns:
        df1_flat = df1_flat.withColumn(col_name, lit(None))  # Add missing column with NULLs
    if col_name not in df2_flat.columns:
        df2_flat = df2_flat.withColumn(col_name, lit(None))  # Add missing column with NULLs

# Step 3: Merge both DataFrames safely
merged_df = df1_flat.unionByName(df2_flat, allowMissingColumns=True)

# Show final merged DataFrame
merged_df.show(truncate=False)

# Print final column names
print(merged_df.columns)


# COMMAND ----------

display(merged_df)

# COMMAND ----------

print(merged_df.columns)

# COMMAND ----------


