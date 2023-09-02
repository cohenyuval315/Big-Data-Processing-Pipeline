
docker build --no-cache -t confluentinc/cp-kafka:latest .
docker run --rm -d --name broker_1 --hostname broker_1 --network kafka confluentinc/cp-kafka:latest
