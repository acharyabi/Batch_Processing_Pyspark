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



    