# Databricks notebook source
# MAGIC %md
# MAGIC ## Extract Parquet
# MAGIC 
# MAGIC I used this notebook to extract Parquet from prior work I was doing. On the original example, the Parquet files were malformed as per this bug (now fixed in DuckDB).
# MAGIC https://github.com/duckdb/duckdb/issues/5575
# MAGIC 
# MAGIC The process I used to create "correct" Parquet was to use a newer version of DuckDB, read the original Parquet and then export it a second time. It's a painful process to go through every time you want to run the benchmark so I've saved the Parquet to my own Google Drive.
# MAGIC 
# MAGIC Another (and potentially better) way to export from a Databricks workspace would be to store these on object storage which would provide faster access to download.

# COMMAND ----------

spark.sql("USE benchmark.tpch_sf10")
tables = ["lineitem", "orders", "nation", "customer", "partsupp", "supplier", "region", "part"]

for table_name in tables:
    print(f"Processing {table_name}")
    df = spark.table(table_name)
    df.coalesce(1).write.format("parquet").mode("overwrite").save(f"file:///tmp/databricks_export/{table_name}")




# COMMAND ----------

import os

base_path = "/tmp/databricks_export/"
target_path = "abfss://landing@dlevy0nlp0storage.dfs.core.windows.net/duckdb_comparison/"
dbutils.fs.mkdirs(target_path)

for table_name in tables:
    print(f"Processing {table_name}")
    parquet_export_path = f"{base_path}{table_name}"
    for file in os.listdir(parquet_export_path):

    # check only text files
        if file.endswith('.parquet'):
            local_path = "file://" + parquet_export_path + "/" + file
            target_file = target_path + table_name + ".parquet"
            print(f"Copying {local_path} to {target_file}")
            dbutils.fs.cp(local_path, target_file)
            break

    

# COMMAND ----------

dbutils.fs.ls("/FileStore/databricks_export")
