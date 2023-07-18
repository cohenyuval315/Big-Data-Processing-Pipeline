

KAFKA_BASE="/usr/local/kafka/kafka"
zookeeper_start_script="$KAFKA_BASE/bin/zookeeper-server-start.sh"
zookeeper_properties="$KAFKA_BASE/config/zookeeper.properties"  
kafka_start_script="$KAFKA_BASE/bin/kafka-server-start.sh"
kafka_properties="$KAFKA_BASE/config/server.properties"

stop_zookeeper() {
  echo "Stopping ZooKeeper server..."
  zookeeper_pid=$(pgrep -f "$zookeeper_start_script $zookeeper_properties")
  if [[ -n $zookeeper_pid ]]; then
    kill -SIGTERM "$zookeeper_pid"
  fi
  wait "$zookeeper_pid" 2>/dev/null
}

stop_kafka() {
  echo "Stopping Kafka broker..."
  kafka_pid=$(pgrep -f "$kafka_start_script $kafka_properties")
  if [[ -n $kafka_pid ]]; then
    kill -SIGTERM "$kafka_pid"
  fi
  wait "$kafka_pid" 2>/dev/null
}

cleanup() {
  stop_kafka
  stop_zookeeper
  exit 0
}

trap cleanup SIGINT


"$zookeeper_start_script" $zookeeper_properties &

sleep 10


"$kafka_start_script" $kafka_properties &

sleep 10

while true; do
  sleep 1
done
