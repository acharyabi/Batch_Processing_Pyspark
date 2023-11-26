#green
types.StructType(
    [
        types.StructField('VendorID', types.IntegerType(), True), 
        types.StructField('lpep_pickup_datetime', types.TimestampType(), True), 
        types.StructField('lpep_dropoff_datetime', types.TimestampType(), True), 
        types.StructField('store_and_fwd_flag', types.StringType(), True), 
        types.StructField('RatecodeID', types.IntegerType(), True), 
        types.StructField('PULocationID', types.IntegerType(), True), 
        types.StructField('DOLocationID', types.IntegerType(), True), 
        types.StructField('passenger_count',types.IntegerType(), True), 
        types.StructField('trip_distance', types.DoubleType(), True), 
        types.StructField('fare_amount', types.DoubleType(), True), 
        types.StructField('extra', types.DoubleType(), True), 
        types.StructField('mta_tax', types.DoubleType(), True), 
        types.StructField('tip_amount', types.DoubleType(), True), 
        types.StructField('tolls_amount', types.DoubleType(), True),
        types.StructField('ehail_fee', types.DoubleType(), True), 
        types.StructField('improvement_surcharge', types.DoubleType(), True), 
        types.StructField('total_amount', types.DoubleType(), True), 
        types.StructField('payment_type', types.IntegerType(), True), 
        types.StructField('trip_type', types.IntegerType(), True), 
        types.StructField('congestion_surcharge', types.DoubleType(), True)
    ]
)
#yellow

types.StructType(
    [
        types.StructField('VendorID', types.IntegerType(), True), 
        types.StructField('tpep_pickup_datetime', types.TimestampType(), True), 
        types.StructField('tpep_dropoff_datetime', types.TimestampType(), True), 
        types.StructField('passenger_count', types.IntegerType(), True), 
        types.StructField('trip_distance', types.DoubleType(), True), 
        types.StructField('RatecodeID', types.IntegerType(), True), 
        types.StructField('store_and_fwd_flag', types.StringType(), True), 
        types.StructField('PULocationID', types.IntegerType(), True), 
        types.StructField('DOLocationID', types.IntegerType(), True), 
        types.StructField('payment_type', types.IntegerType(), True), 
        types.StructField('fare_amount', types.DoubleType(), True), 
        types.StructField('extra', types.DoubleType(), True), 
        types.StructField('mta_tax', types.DoubleType(), True), 
        types.StructField('tip_amount', types.DoubleType(), True), 
        types.StructField('tolls_amount', types.DoubleType(), True), 
        types.StructField('improvement_surcharge', types.DoubleType(), True), 
        types.StructField('total_amount', types.DoubleType(), True), 
        types.StructField('congestion_surcharge', types.DoubleType(), True)
    ]
)

{{ config(materialized='table') }}

with trips_data AS (
    SELECT * FROM {{ ref('fact_trips') }}
)
    --Revenue grouping.
    SELECT 
        pickup_zone AS revenue_zone,
        date_trunc('month', pickup_datetime) AS revenue_month, 
        service_type, 

    --Revenue calculation.
        SUM(fare_amount) AS revenue_monthly_fare,
        SUM(extra) AS revenue_monthly_extra,
        SUM(mta_tax) AS revenue_monthly_mta_tax,
        SUM(tip_amount) AS revenue_monthly_tip_amount,
        SUM(tolls_amount) AS revenue_monthly_tolls_amount,
        SUM(ehail_fee) AS  revenue_monthly_ehail_fee,
        SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
        SUM(total_amount) AS revenue_monthly_total_amount,
        SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,
    
    --Additional calculations.
        COUNT(tripid) AS total_monthly_trips,
        AVG(passenger_count) AS avg_montly_passenger_count,
        AVG(trip_distance) AS avg_montly_trip_distance

    FROM 
        trips_data
    GROUP BY 
        1,2,3