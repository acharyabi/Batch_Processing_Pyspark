##Initializing PySpark##
export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9-src.zip:$PYTHONPATH"export JAVA_HOME="${HOME}/spark/jdk-11.0.2"
export PATH="${JAVA_HOME}/bin:${PATH}"

export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9-src.zip:$PYTHONPATH"
export JAVA_HOME="${HOME}/spark/jdk-11.0.2"
export PATH="${JAVA_HOME}/bin:${PATH}"

Location of the connector in google cloud storage. 
Code: gsutil cp gs://hadoop-lib/gcs/gcs-connector-hadoop3-3.4.1.jar

https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar

https://spark.apache.org/docs/latest/spark-standalone.html


Starting the spark:
#Move to the spark directory: echo $SPARK_HOME
./sbin/start-master.sh

#For checking which port has been used for the spark job cluster.
cat /home/abinash/spark/spark-3.4.1-bin-hadoop3/logs/spark-abinash-org.apache.spark.deploy.master.Master-1-de-zoomcamp.out

#Master:
spark://de-zoomcamp.asia-south2-a.c.dtc-abi-tyingtolearn.internal:7077

#Set the worker for handling the job.
./sbin/start-worker.sh spark://de-zoomcamp.asia-south2-a.c.dtc-abi-tyingtolearn.internal:7077

