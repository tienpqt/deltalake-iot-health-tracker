-- Databricks notebook source
Select * from diamonds;

-- COMMAND ----------

drop table if exists diamonds;

-- COMMAND ----------

create table diamonds 
using csv 
options (path "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv",header = "true")

-- COMMAND ----------

select * from diamonds

-- COMMAND ----------

-- MAGIC %python
-- MAGIC diamonds=spark.read.csv("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header="True",inferSchema="true")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC diamonds

-- COMMAND ----------

-- MAGIC %python
-- MAGIC diamonds.write.format("delta").save("/delta/diamonds2")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import avg
-- MAGIC 
-- MAGIC display(diamonds.select("color","price").groupBy("color").agg(avg("price")).sort("color"))

-- COMMAND ----------

create database if not exists dbacademy;

-- COMMAND ----------

use dbacademy

-- COMMAND ----------

set spark.sql.shuffle.partitions=8

-- COMMAND ----------

select * from health_tracker_data_2020_02

-- COMMAND ----------

use default;

-- COMMAND ----------

SELECT * FROM health_tracker_data_2020_01

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls /dbacademy/DLRS/healthtracker/silver

-- COMMAND ----------

DROP TABLE IF EXISTS health_tracker_silver;               -- ensures that if we run this again, it won't fail
                                                          
CREATE TABLE health_tracker_silver                        
USING PARQUET                                             
PARTITIONED BY (p_device_id)                              -- column used to partition the data
LOCATION "/dbacademy/DLRS/healthtracker/silver"           -- location where the parquet files will be saved
AS (                                                      
  SELECT name,                                            -- query used to transform the raw data
         heartrate,                                       
         CAST(FROM_UNIXTIME(time) AS TIMESTAMP) AS time,  
         CAST(FROM_UNIXTIME(time) AS DATE) AS dte,        
         device_id AS p_device_id                         
  FROM health_tracker_data_2020_01   
)

-- COMMAND ----------

select count(*) from health_tracker_silver

-- COMMAND ----------

describe detail health_tracker_silver

-- COMMAND ----------

CONVERT TO DELTA 
  parquet.`/dbacademy/DLRS/healthtracker/silver` 
  PARTITIONED BY (p_device_id double)

-- COMMAND ----------

-- MAGIC 
-- MAGIC %fs
-- MAGIC ls /dbacademy/DLRS/healthtracker/silver

-- COMMAND ----------

DROP TABLE IF EXISTS health_tracker_silver;

CREATE TABLE health_tracker_silver
USING DELTA
LOCATION "/dbacademy/DLRS/healthtracker/silver"

-- COMMAND ----------

describe detail health_tracker_silver

-- COMMAND ----------

SELECT COUNT(*) FROM health_tracker_silver

-- COMMAND ----------

create table health_tracker_user_analytics
using delta 
location '/dbacademy/DLRS/healthtracker/gold/health_tracker_user_analytics'
as (
  select p_device_id,
         avg(heartrate) as avg_heartrate,
         std(heartrate) as std_heartrate,
         max(heartrate) as max_heartrate
       from health_tracker_silver group by p_device_id
                  
)

-- COMMAND ----------

select * from health_tracker_user_analytics

-- COMMAND ----------

INSERT INTO health_tracker_silver
SELECT name,
       heartrate,
       CAST(FROM_UNIXTIME(time) AS TIMESTAMP) AS time,
       CAST(FROM_UNIXTIME(time) AS DATE) AS dte,
       device_id as p_device_id
FROM health_tracker_data_2020_02

-- COMMAND ----------

select * from health_tracker_silver

-- COMMAND ----------

select count(*) from health_tracker_silver version 

-- COMMAND ----------

select p_device_id, count(*) from health_tracker_silver version as of 0 group by (p_device_id)

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW broken_readings
AS (
  SELECT COUNT(*) as broken_readings_count, dte FROM health_tracker_silver
  WHERE heartrate < 0
  GROUP BY dte
  ORDER BY dte
)

-- COMMAND ----------

select * from broken_readings

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------

select sum(broken_readings_count) from broken_readings;

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW updates 
AS (
  SELECT name, (prev_amt+next_amt)/2 AS heartrate, time, dte, p_device_id
  FROM (
    SELECT *, 
    LAG(heartrate) OVER (PARTITION BY p_device_id, dte ORDER BY p_device_id, dte) AS prev_amt, 
    LEAD(heartrate) OVER (PARTITION BY p_device_id, dte ORDER BY p_device_id, dte) AS next_amt 
    FROM health_tracker_silver
  ) 
  WHERE heartrate < 0
)

-- COMMAND ----------

describe health_tracker_silver

-- COMMAND ----------

describe updates

-- COMMAND ----------

describe history health_tracker_silver

-- COMMAND ----------


