IMAGE=python:3.11
NAME=webServer
PORT=7676

docker webServer || true
docker rm -f webServer || true
docker build --no-cache -t python:3.11 . || true
docker network create --label=clientNet application 2>/dev/null || true
docker run --rm -d --name webServer --hostname webServer --network cassandra,application -p 7676:7676 python:3.11  || true
