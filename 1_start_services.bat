@echo off
start "Zookeeper" cmd /k "D: & cd \ & kafka\kafka_2.13-3.5.0\bin\windows\zookeeper-server-start.bat kafka\kafka_2.13-3.5.0\config\zookeeper.properties"
start "Kafka" cmd /k "D: & cd \ && kafka\kafka_2.13-3.5.0\bin\windows\kafka-server-start.bat kafka\kafka_2.13-3.5.0\config\server.properties"
start "Spark Master" cmd /k "D: & cd \ & spark\spark-3.4.1-bin-hadoop3\bin\spark-class org.apache.spark.deploy.master.Master --host 0.0.0.0"
start "Spark Worker" cmd /k "D: & cd \ & spark\spark-3.4.1-bin-hadoop3\bin\spark-class org.apache.spark.deploy.worker.Worker spark://127.0.0.1:7078 --host 127.0.0.1"
