docker build --no-cache -t confluentinc/cp-zookeeper:latest .
docker network create --label=kafkaNet kafka 2>/dev/null || true
docker run --rm -d --name zookeeper_1 --hostname zookeeper_1 --network kafka confluentinc/cp-zookeeper
