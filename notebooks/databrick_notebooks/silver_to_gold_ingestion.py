# Databricks notebook source
dbutils.widgets.text("gold_table", "")
gold_table = dbutils.widgets.get("gold_table")

dbutils.widgets.text("gold_schema", "")
gold_schema = dbutils.widgets.get("gold_schema")

dbutils.widgets.text("silver_table", "")
silver_table = dbutils.widgets.get("silver_table")

dbutils.widgets.text("silver_schema", "")
silver_schema = dbutils.widgets.get("silver_schema")

dbutils.widgets.text("LoadID", "")
LoadID = dbutils.widgets.get("LoadID")

dbutils.widgets.text("source", "")
source = dbutils.widgets.get("source")


# COMMAND ----------

if(source == 'sftp_host'):
    df = spark.sql(f"""

        with 
        temp_petrol as (select * from {silver_schema}.fact_transport_petrol where load_id = '{LoadID}'),
        temp_diesel as (select * from {silver_schema}.fact_transport_diesel where load_id = '{LoadID}'),
        temp_electric as (select * from {silver_schema}.fact_transport_electric where load_id = '{LoadID}'),
        temp_others as (select * from {silver_schema}.fact_transport_others where load_id = '{LoadID}')

        select 
        temp_petrol.dist_code,
        temp_petrol.month, 
        temp_petrol.fuel_type_petrol, 
        temp_diesel.fuel_type_diesel, 
        temp_electric.fuel_type_electric, 
        temp_others.fuel_type_others, 
        temp_petrol.vehicleClass_MotorCycle,
        temp_petrol.vehicleClass_MotorCar,
        temp_petrol.vehicleClass_AutoRickshaw,
        temp_petrol.vehicleClass_Agriculture,
        temp_petrol.vehicleClass_others,
        temp_petrol.seatCapacity_1_to_3,
        temp_petrol.seatCapacity_4_to_6,
        temp_petrol.seatCapacity_above_6,
        temp_petrol.Brand_new_vehicles,
        temp_petrol.`Pre-owned_vehicles`,
        temp_petrol.`category_Non-Transport`,
        temp_petrol.category_Transport,
        temp_petrol.seq_no,
        from_utc_timestamp(now()) as last_insert_dttm,
        temp_petrol.load_id
        from temp_petrol join temp_diesel on temp_petrol.seq_no = temp_diesel.seq_no 
        join temp_electric on temp_diesel.seq_no = temp_electric.seq_no 
        join temp_others on temp_electric.seq_no = temp_others.seq_no
    """)
    

# COMMAND ----------

if(source == 'mysql_host'):
    df = spark.sql(f"select * from {silver_schema}.{silver_table}")

# COMMAND ----------

#insert silver layer table records into gold layer table records
df.write.mode('overwrite').saveAsTable(f"{gold_schema}.{gold_table}")

gold_table_record_count = df.count()

# COMMAND ----------

#return records to azure data factory
dbutils.notebook.exit(gold_table_record_count)