FROM  apache/spark-py
EXPOSE 8080 7078 7077
CMD ["/bin/spark-class org.apache.spark.deploy.worker.Worker","spark://spark-master:7077"]