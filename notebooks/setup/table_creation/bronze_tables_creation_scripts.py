# Databricks notebook source
dbutils.widgets.text("storage_account_name", "")
storage_account_name = dbutils.widgets.get("storage_account_name")

dbutils.widgets.text("env", "")
env = dbutils.widgets.get("env")

# COMMAND ----------

spark.sql(f"""

create database if not exists hive_metastore.{env}_bronze         
""")


# COMMAND ----------

spark.sql(f"""
          
create or replace table {env}_bronze.fact_transport_diesel (

    dist_code string,
    month string,
    fuel_type_diesel string,
    vehicleClass_MotorCycle string,
    vehicleClass_MotorCar string,
    vehicleClass_AutoRickshaw string,
    vehicleClass_Agriculture string,
    vehicleClass_others string,
    seatCapacity_1_to_3 string,
    seatCapacity_4_to_6 string,
    seatCapacity_above_6 string,
    Brand_new_vehicles string,
    `Pre-owned_vehicles` string,
    `category_Non-Transport` string,
    category_Transport string,
    seq_no string,
    last_insert_dttm string,
    load_id string
)      
location 'abfss://bronze@{storage_account_name}.dfs.core.windows.net/fact_transport_diesel'    
""")

# COMMAND ----------

spark.sql(f"""
          
create or replace table {env}_bronze.fact_transport_electric (
    
    dist_code string,
    month string,
    fuel_type_electric string,
    vehicleClass_MotorCycle string,
    vehicleClass_MotorCar string,
    vehicleClass_AutoRickshaw string,
    vehicleClass_Agriculture string,
    vehicleClass_others string,
    seatCapacity_1_to_3 string,
    seatCapacity_4_to_6 string,
    seatCapacity_above_6 string,
    Brand_new_vehicles string,
    `Pre-owned_vehicles` string,
    `category_Non-Transport` string,
    category_Transport string,
    seq_no string, 
    last_insert_dttm string,
    load_id string
)      
location 'abfss://bronze@{storage_account_name}.dfs.core.windows.net/fact_transport_electric'    
""")

# COMMAND ----------

spark.sql(f"""
          
create or replace table {env}_bronze.fact_transport_others (
    
    dist_code string,
    month string,
    fuel_type_others string,
    vehicleClass_MotorCycle string,
    vehicleClass_MotorCar string,
    vehicleClass_AutoRickshaw string,
    vehicleClass_Agriculture string,
    vehicleClass_others string,
    seatCapacity_1_to_3 string,
    seatCapacity_4_to_6 string,
    seatCapacity_above_6 string,
    Brand_new_vehicles string,
    `Pre-owned_vehicles` string,
    `category_Non-Transport` string,
    category_Transport string,
    seq_no string,
    last_insert_dttm string,
    load_id string
)      
location 'abfss://bronze@{storage_account_name}.dfs.core.windows.net/fact_transport_others'    
""")

# COMMAND ----------

spark.sql(f"""
          
create or replace table {env}_bronze.fact_transport_petrol (
    
    dist_code string,
    month string,
    fuel_type_petrol string,
    vehicleClass_MotorCycle string,
    vehicleClass_MotorCar string,
    vehicleClass_AutoRickshaw string,
    vehicleClass_Agriculture string,
    vehicleClass_others string,
    seatCapacity_1_to_3 string,
    seatCapacity_4_to_6 string,
    seatCapacity_above_6 string,
    Brand_new_vehicles string,
    `Pre-owned_vehicles` string,
    `category_Non-Transport` string,
    category_Transport string,
    seq_no string,
    last_insert_dttm string,
    load_id string
)      
location 'abfss://bronze@{storage_account_name}.dfs.core.windows.net/fact_transport_petrol'    
""")

# COMMAND ----------

spark.sql(f"""
          
create or replace table {env}_bronze.dim_date (
    
    month string,
    Mmm string,
    quarter string,
    fiscal_year int,
    last_insert_dttm string,
    load_id string
)      
location 'abfss://bronze@{storage_account_name}.dfs.core.windows.net/dim_date'    
""")

# COMMAND ----------

spark.sql(f"""
          
create or replace table {env}_bronze.dim_districts (
    
    dist_code string,
    district string,
    last_insert_dttm string,
    load_id string
)      
location 'abfss://bronze@{storage_account_name}.dfs.core.windows.net/dim_districts'    
""")

# COMMAND ----------

spark.sql(f"""
          
create or replace table {env}_bronze.fact_stamps (
    
    dist_code string,
    month string,
    documents_registered_cnt int,
    documents_registered_rev double,
    estamps_challans_cnt int,
    estamps_challans_rev int,
    last_insert_dttm string,
    load_id string
)      
location 'abfss://bronze@{storage_account_name}.dfs.core.windows.net/fact_stamps'    
""")

# COMMAND ----------

spark.sql(f"""
          
create or replace table {env}_bronze.fact_ts_ipass (
    
    dist_code string,
    month string,
    sector string,
    investment_in_cr double,
    number_of_employees int,
    last_insert_dttm string,
    load_id string
)      
location 'abfss://bronze@{storage_account_name}.dfs.core.windows.net/fact_ts_ipass'    
""")