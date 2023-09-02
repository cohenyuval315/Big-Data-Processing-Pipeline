docker build --no-cache -t python:3.11 .
docker run --rm -d --name sources_1 --hostname sources_1 --network kafka python:3.11
