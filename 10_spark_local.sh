python3 10_spark_local.py\
    --input_green=data/pq/green/2020/* \
    --input_yellow=data/pq/yellow/2020/*\
    --output=data/report-2020

URL="spark://de-zoomcamp.asia-south2-a.c.dtc-abi-tyingtolearn.internal:7077"

spark-submit\
    --master="${URL}" \
    10_spark_local.py \
        --input_green=data/pq/green/2021/* \
        --input_yellow=data/pq/yellow/2021/*\
        --output=data/report-2021

# For using in cluster argument.

--input_green=gs://dtc_data_lake_dtc-abi-tyingtolearn/pq/green/2021/*/ \
--input_yellow=gs://dtc_data_lake_dtc-abi-tyingtolearn/pq/yellow/2021/*/\
--output=gs://dtc_data_lake_dtc-abi-tyingtolearn/report-2021

#Bucket-name.
dtc_data_lake_dtc-abi-tyingtolearn/

gcloud dataproc jobs submit pyspark \
    --cluster=dezoomcamp-cluster \
    --region=us-central1 \
    gs://dtc_data_lake_dtc-abi-tyingtolearn/code/10_spark_local.py \
    -- \
        --input_green=gs://dtc_data_lake_dtc-abi-tyingtolearn/pq/green/2021/*/ \
        --input_yellow=gs://dtc_data_lake_dtc-abi-tyingtolearn/pq/yellow/2021/*/\
        --output=gs://dtc_data_lake_dtc-abi-tyingtolearn/report-2021


gcloud dataproc jobs submit pyspark \
    --cluster=dezoomcamp-cluster \
    --region=us-central1 \
    gs://dtc_data_lake_dtc-abi-tyingtolearn/code/10_spark_local.py \
    -- \
        --input_green=gs://dtc_data_lake_dtc-abi-tyingtolearn/pq/green/2020/*/ \
        --input_yellow=gs://dtc_data_lake_dtc-abi-tyingtolearn/pq/yellow/2020/*/ \
        --output=gs://dtc_data_lake_dtc-abi-tyingtolearn/report-2020

https://cloud.google.com/dataproc/docs/tutorials/bigquery-connector-spark-example 

#Trips_data_all is the schema name.
trips_data_all.reports-2020

#For using spark directly using daraproc to the bigquery.
gcloud dataproc jobs submit pyspark \
    --jars=gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.11-0.23.2.jar \
    --cluster=dezoomcamp-cluster \
    --region=us-central1 \
    gs://dtc_data_lake_dtc-abi-tyingtolearn/code/11_spark_bigquery.py \
    -- \
        --input_green=gs://dtc_data_lake_dtc-abi-tyingtolearn/pq/green/2021/*/ \
        --input_yellow=gs://dtc_data_lake_dtc-abi-tyingtolearn/pq/yellow/2021/*/ \
        --output=trips_data_all.reports-2021
