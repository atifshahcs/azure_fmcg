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

# COMMAND ----------

#storing source_file data in a dataframe
file_path = f"abfss://landing@{storage_account_name}.dfs.core.windows.net/target_sftp_files/{source_file_name}"
df = spark.read.csv(file_path, header = 'true')
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