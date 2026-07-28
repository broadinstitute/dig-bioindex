# 3.11 suits the pins in requirements.txt: orjson 3.11.6 publishes no wheel
# below 3.10, and botocore 1.20 installs on 3.12 but dies importing
# botocore.vendored.six.moves. That is a constraint of these pins rather than
# of bioindex, which also runs on 3.12 against a newer resolved set - see
# python_requires in setup.py.
FROM --platform=linux/amd64 python:3.11-slim AS build

RUN apt-get update && \
    apt-get install -y default-libmysqlclient-dev pkg-config build-essential

COPY requirements.txt .
COPY bioindex ./bioindex
COPY batch-index-files/index_files.py .
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install mysqlclient


