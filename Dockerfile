# Bounded on both sides: orjson 3.11.6 publishes no wheel below 3.10, and
# botocore 1.20 installs on 3.12 but dies importing botocore.vendored.six.moves.
FROM --platform=linux/amd64 python:3.11-slim AS build

RUN apt-get update && \
    apt-get install -y default-libmysqlclient-dev pkg-config build-essential

COPY requirements.txt .
COPY bioindex ./bioindex
COPY batch-index-files/index_files.py .
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install mysqlclient


