FROM confluentinc/cp-kafka:latest
COPY ./config/server.properties /etc/kafka/server.properties
CMD ["kafka-server-start", "/etc/kafka/server.properties"]