# Databricks notebook source
dbutils.widgets.text("storage_account_name", "")
storage_account_name = dbutils.widgets.get("storage_account_name")

dbutils.widgets.text("env", "")
env = dbutils.widgets.get("env")

# COMMAND ----------

spark.sql(f"""

create database if not exists hive_metastore.{env}_silver      
""")


# COMMAND ----------

spark.sql(f"""
          
create or replace table {env}_silver.fact_transport_petrol (

    dist_code string,
    month date,
    fuel_type_petrol int,
    vehicleClass_MotorCycle int,
    vehicleClass_MotorCar int,
    vehicleClass_AutoRickshaw int,
    vehicleClass_Agriculture int,
    vehicleClass_others int,
    seatCapacity_1_to_3 int,
    seatCapacity_4_to_6 int,
    seatCapacity_above_6 int,
    Brand_new_vehicles int,
    `Pre-owned_vehicles` int,
    `category_Non-Transport` int,
    category_Transport int,
    seq_no int,
    last_insert_dttm timestamp,
    load_id string
)      
location 'abfss://silver@{storage_account_name}.dfs.core.windows.net/fact_transport_petrol'    
""")

# COMMAND ----------

spark.sql(f"""
          
create or replace table {env}_silver.fact_transport_diesel (

    dist_code string,
    month date,
    fuel_type_diesel int,
    vehicleClass_MotorCycle int,
    vehicleClass_MotorCar int,
    vehicleClass_AutoRickshaw int,
    vehicleClass_Agriculture int,
    vehicleClass_others int,
    seatCapacity_1_to_3 int,
    seatCapacity_4_to_6 int,
    seatCapacity_above_6 int,
    Brand_new_vehicles int,
    `Pre-owned_vehicles` int,
    `category_Non-Transport` int,
    category_Transport int,
    seq_no int,
    last_insert_dttm timestamp,
    load_id string
)      
location 'abfss://silver@{storage_account_name}.dfs.core.windows.net/fact_transport_diesel'    
""")

# COMMAND ----------

spark.sql(f"""
          
create or replace table {env}_silver.fact_transport_electric (

    dist_code string,
    month date,
    fuel_type_electric int,
    vehicleClass_MotorCycle int,
    vehicleClass_MotorCar int,
    vehicleClass_AutoRickshaw int,
    vehicleClass_Agriculture int,
    vehicleClass_others int,
    seatCapacity_1_to_3 int,
    seatCapacity_4_to_6 int,
    seatCapacity_above_6 int,
    Brand_new_vehicles int,
    `Pre-owned_vehicles` int,
    `category_Non-Transport` int,
    category_Transport int,
    seq_no int,
    last_insert_dttm timestamp,
    load_id string
)      
location 'abfss://silver@{storage_account_name}.dfs.core.windows.net/fact_transport_electric'    
""")

# COMMAND ----------

spark.sql(f"""
          
create or replace table {env}_silver.fact_transport_others (

    dist_code string,
    month date,
    fuel_type_others int,
    vehicleClass_MotorCycle int,
    vehicleClass_MotorCar int,
    vehicleClass_AutoRickshaw int,
    vehicleClass_Agriculture int,
    vehicleClass_others int,
    seatCapacity_1_to_3 int,
    seatCapacity_4_to_6 int,
    seatCapacity_above_6 int,
    Brand_new_vehicles int,
    `Pre-owned_vehicles` int,
    `category_Non-Transport` int,
    category_Transport int,
    seq_no int,
    last_insert_dttm timestamp,
    load_id string
)      
location 'abfss://silver@{storage_account_name}.dfs.core.windows.net/fact_transport_others'    
""")

# COMMAND ----------

spark.sql(f"""
          
create or replace table {env}_silver.dim_date (
    
    month date,
    Mmm string,
    quarter string,
    fiscal_year int,
    last_insert_dttm timestamp,
    load_id string
)      
location 'abfss://silver@{storage_account_name}.dfs.core.windows.net/dim_date'    
""")

# COMMAND ----------

spark.sql(f"""
          
create or replace table {env}_silver.dim_districts (
    
    dist_code string,
    district string,
    last_insert_dttm timestamp,
    load_id string
)      
location 'abfss://silver@{storage_account_name}.dfs.core.windows.net/dim_districts'    
""")

# COMMAND ----------

spark.sql(f"""
          
create or replace table {env}_silver.fact_stamps (
    
    dist_code string,
    month date,
    documents_registered_cnt int,
    documents_registered_rev double,
    estamps_challans_cnt int,
    estamps_challans_rev int,
    last_insert_dttm timestamp,
    load_id string
)      
location 'abfss://silver@{storage_account_name}.dfs.core.windows.net/fact_stamps'    
""")

# COMMAND ----------

spark.sql(f"""
          
create or replace table {env}_silver.fact_ts_ipass (
    
    dist_code string,
    month date,
    sector string,
    investment_in_cr double,
    number_of_employees int,
    last_insert_dttm timestamp,
    load_id string
)      
location 'abfss://silver@{storage_account_name}.dfs.core.windows.net/fact_ts_ipass'    
""")