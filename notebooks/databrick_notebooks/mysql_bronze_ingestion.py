# Databricks notebook source
dbutils.widgets.text("bronze_table", "")
bronze_table = dbutils.widgets.get("bronze_table")

dbutils.widgets.text("bronze_schema", "")
bronze_schema = dbutils.widgets.get("bronze_schema")

dbutils.widgets.text("source_file_name", "")
source_file_name = dbutils.widgets.get("source_file_name")

dbutils.widgets.text("storage_account_name", "")
storage_account_name = dbutils.widgets.get("storage_account_name")

dbutils.widgets.text("LoadID", "")
LoadID = dbutils.widgets.get("LoadID")

dbutils.widgets.text("env", "")
env = dbutils.widgets.get("env")

# COMMAND ----------

#storing source_file data in a dataframe
file_path = f"abfss://landing@{storage_account_name}.dfs.core.windows.net/target_mysql_files/{source_file_name}"
df = spark.read.parquet(file_path, header = 'true')

# COMMAND ----------

from pyspark.sql.functions import col

first_column_name = df.columns[0]
first_col_value = df.collect()[0][0]

# Filter out the first row where the value in the first column matches the column name
df = df.filter(col(first_column_name) != first_col_value)
source_file_records_count = df.count()

#creating temporary view from the df dataframe
df.createOrReplaceTempView('temp_view')

# COMMAND ----------

#insert records into bronze layer table
spark.sql(f"""
          
    insert into {bronze_schema}.{bronze_table} select *, from_utc_timestamp(now(), 'IST'), '{LoadID}' from temp_view;
""")

# COMMAND ----------

bronze_tbl_count = spark.sql(f"select * from {bronze_schema}.{bronze_table} where load_id = '{LoadID}'").count()

# COMMAND ----------

#return records to azure data factory
dbutils.notebook.exit([source_file_records_count, bronze_tbl_count])