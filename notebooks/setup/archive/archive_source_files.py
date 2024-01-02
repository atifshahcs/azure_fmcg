# Databricks notebook source
dbutils.widgets.text("LoadID", "")
LoadID = dbutils.widgets.get("LoadID")

dbutils.widgets.text("source_file_name", "")
source_file_name = dbutils.widgets.get("source_file_name")

dbutils.widgets.text("storage_account_name", "")
storage_account_name = dbutils.widgets.get("storage_account_name")

dbutils.widgets.text("file_path", "")
file_path = dbutils.widgets.get("file_path")

# COMMAND ----------

adls_source_file_path = f"abfss://landing@{storage_account_name}.dfs.core.windows.net/{file_path}/{source_file_name}"
adls_archive_file_path = f"abfss://landing@{storage_account_name}.dfs.core.windows.net/archive/{source_file_name}_{LoadID}"

# COMMAND ----------

#move source file to archive location
dbutils.fs.mv(adls_source_file_path,adls_archive_file_path)