


# stop container
docker stop cassandra || true

# remove container
docker rm cassandra || true

# docker kill cassandra || true

# docker network rm cassandra || true

# clean image
docker rmi $(docker images | grep 'cassandra:latest') || true

# clean network
docker system prune --filter "label==cassandaNet" || true

# build image
docker build --no-cache -t cassandra:latest . || true

# create a Docker network
docker network create --label=cassandaNet cassandra 2>/dev/null || true

# run container
docker run --rm -d --name cassandra --hostname cassandra --network cassandra -p 7000:7000 -p 7199:7199 -p 9042:9042 cassandra:latest  || true


sleep 120

docker exec -it cassandra bash -c "bash /var/lib/cassandra/scripts/create_db.sh"