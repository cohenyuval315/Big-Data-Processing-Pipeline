#!/bin/bash

# Set Spark home directory
SPARK_HOME=/usr/local/spark/spark

# Start the Spark master
$SPARK_HOME/sbin/start-master.sh

# Start the Spark worker
$SPARK_HOME/sbin/start-worker.sh spark://localhost:7077

# Keep the script running to keep the Spark cluster alive
while true; do sleep 1; done
