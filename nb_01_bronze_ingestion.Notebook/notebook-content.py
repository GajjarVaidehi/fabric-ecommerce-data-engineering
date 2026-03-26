# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "60d1e55b-ad26-40c0-b3d3-39c8508f5b41",
# META       "default_lakehouse_name": "e_commerce_data",
# META       "default_lakehouse_workspace_id": "9672d9c4-5d16-481f-bb62-d8e5f047ca49",
# META       "known_lakehouses": [
# META         {
# META           "id": "60d1e55b-ad26-40c0-b3d3-39c8508f5b41"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql.functions import current_timestamp, input_file_name, lit

base_path = "Files/bronze/raw/"

tables = [
    "customers",
    "orders",
    "order_items",
    "payments",
    "products",
    "reviews"
]

for table in tables:
    df = spark.read.option("header", True).csv(f"{base_path}{table}.csv")
    
    df = df.withColumn("ingestion_ts", current_timestamp()) \
           .withColumn("source_file", input_file_name()) \
           .withColumn("batch_id", lit("batch_001"))

    df.write.format("delta").mode("overwrite").saveAsTable(f"brz_{table}")

    print(f"{table} loaded successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM dbo.brz_orders;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
