ARG VERSION=3.11
FROM python:$VERSION
ENV SERVER_PORT=7676
ENV SERVER_HOST=0.0.0.0
WORKDIR /usr/src/server
COPY . /usr/src/server/
RUN pip install --no-cache-dir -r /usr/src/server/requirements.txt
EXPOSE 7676
CMD ["python","-u", "/usr/src/server/server.py"]