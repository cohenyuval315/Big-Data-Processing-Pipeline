FROM  apache/spark-py
COPY . /usr/src/process
EXPOSE 8080 7078 7077 9090
CMD ["bin/spark-class org.apache.spark.deploy.master.Master"]