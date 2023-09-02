# ARG api
# ARG secret
ARG VERSION=3.11
FROM python:$VERSION
# ENV BINANCE_API_KEY=${api}
# ENV BINANCE_SECRET_KEY=${secret}
WORKDIR /usr/src/sources_1
COPY . /usr/src/sources_1/
RUN pip install --no-cache-dir -r /usr/src/sources_1/requirements.txt
CMD ["python","-u", "binance/async_real_time_data_kafka.py"]
