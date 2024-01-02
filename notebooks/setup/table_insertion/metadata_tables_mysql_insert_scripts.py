# Databricks notebook source
dbutils.widgets.text("env", "")
env = dbutils.widgets.get("env")

dbutils.widgets.text("logic_app_url", "")
logic_app_url = dbutils.widgets.get("logic_app_url")

dbutils.widgets.text("email_id", "")
email_id = dbutils.widgets.get("email_id")

dbutils.widgets.text("storage_account", "")
storage_account = dbutils.widgets.get("storage_account")

dbutils.widgets.text("adls_url", "")
adls_url = dbutils.widgets.get("adls_url")

# COMMAND ----------

#job_id 102 
# tbl_source
spark.sql(f"""
insert into metadata_schema.tbl_source_control values ('mysql_host',102,'localhost',NULL,3306,'fmcg','root','mysql-password','{storage_account}','{adls_url}','landing','{logic_app_url}','{email_id}')        
""")

# COMMAND ----------

# spark.sql(f"""
# Delete from metadata_schema.tbl_parameters
# """)

# COMMAND ----------

#job_id 102
spark.sql(f"""
insert into metadata_schema.tbl_parameters values 
(102,'dim_date',NULL,'target_mysql_files','{env}_bronze','dim_date','{env}_silver','dim_date','{env}_gold','dim_date'),
(102,'dim_districts',NULL,'target_mysql_files','{env}_bronze','dim_districts','{env}_silver','dim_districts','{env}_gold','dim_districts'),
(102,'fact_stamps',NULL,'target_mysql_files','{env}_bronze','fact_stamps','{env}_silver','fact_stamps','{env}_gold','fact_stamps'),
(102,'fact_ts_ipass',NULL,'target_mysql_files','{env}_bronze','fact_ts_ipass','{env}_silver','fact_ts_ipass','{env}_gold','fact_ts_ipass')
""")