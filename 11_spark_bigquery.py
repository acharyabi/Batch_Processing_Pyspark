#!/usr/bin/env python
# coding: utf-8
import argparse
import pyspark
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()

parser.add_argument('--input_green', required=True)
parser.add_argument('--input_yellow', required=True, help='password for postgres')
parser.add_argument('--output', required=True, help='port for postgres')

args= parser.parse_args()

#Taking arguments for input and output.
input_green = args.input_green
input_yellow = args.input_yellow
output = args.output

#### The master isn't specified so that the dataproc can automatically create the master as configured. ####
spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()

#  Use the Cloud Storage bucket for temporary BigQuery export data used
#  by the connector.
bucket = "dataproc-temp-us-central1-546744351946-asb2i9uc"
spark.conf.set("temporaryGcsBucket", bucket)

df_green=spark.read.parquet(input_green)
df_yellow=spark.read.parquet(input_yellow)

df_yellow= df_yellow \
    .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime')\
    .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')


df_green=df_green \
    .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime')\
    .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')


#seeing the columns that are common in both the datasets.
set(df_green.columns)&set(df_yellow.columns)

#Since they don't have order maintained in the above method we want to maintain the order in which column is presented.
common_columns=[]

yellow_columns = set(df_yellow.columns)

for col in df_green.columns:
    if col in yellow_columns:
        common_columns.append(col)

common_columns

from pyspark.sql import functions as F

df_green_sel=df_green\
    .select(common_columns)\
    .withColumn('service_type',F.lit('green'))

df_yellow_sel=df_yellow\
    .select(common_columns)\
    .withColumn('service_type',F.lit('yellow'))

df_trips_data = df_green_sel.unionAll(df_yellow_sel)

df_trips_data.groupBy('service_type').count().show()

#Making it accessible for the sql.
df_trips_data.registerTempTable('trips_data')

spark.sql("""
SELECT 
    service_type,
    count(1)
FROM trips_data 
GROUP BY
    service_type;  
""").show()

df_result = spark.sql("""
SELECT 
    -- Reveneue grouping 
    PULocationID AS revenue_zone,
    date_trunc('month', pickup_datetime) AS revenue_month, 
    service_type, 

    -- Revenue calculation 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

    -- Additional calculations
    AVG(passenger_count) AS avg_montly_passenger_count,
    AVG(trip_distance) AS avg_montly_trip_distance
FROM
    trips_data
GROUP BY
    1, 2, 3
""")
df_result.show()

#To reduce the number of files to only one.
# df_result.coalesce(1).write.parquet(output, mode='overwrite')
#  Saving the data to BigQuery.
df_result.write.format("bigquery")\
  .option("table",output)\
  .mode("overwrite") \
  .save()
