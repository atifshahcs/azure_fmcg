# Databricks notebook source
dbutils.widgets.text("bronze_table", "")
bronze_table = dbutils.widgets.get("bronze_table")

dbutils.widgets.text("bronze_schema", "")
bronze_schema = dbutils.widgets.get("bronze_schema")

dbutils.widgets.text("silver_table", "")
silver_table = dbutils.widgets.get("silver_table")

dbutils.widgets.text("silver_schema", "")
silver_schema = dbutils.widgets.get("silver_schema")

dbutils.widgets.text("LoadID", "")
LoadID = dbutils.widgets.get("LoadID")

# COMMAND ----------

df = spark.sql(f"select * from {bronze_schema}.{bronze_table} where load_id = '{LoadID}'")
pre_count = df.count()

#removing duplicate values from the dataframe
df = df.dropDuplicates()

duplicate_records_count = pre_count - df.count()

# COMMAND ----------

#Column datatypes updation

from pyspark.sql.functions import col
df_schema = spark.sql(f"describe table {silver_schema}.{silver_table}")

for row in df_schema.collect():
    col_name = row['col_name']
    data_type = row['data_type']
    df = df.withColumn(col_name, col(col_name).cast(data_type))

# COMMAND ----------

#insert bronze layer table records into silver layer table records
df.write.mode('overwrite').saveAsTable(f"{silver_schema}.{silver_table}")

silver_table_record_count = df.count()

# COMMAND ----------

#return records to azure data factory
dbutils.notebook.exit([duplicate_records_count, silver_table_record_count])