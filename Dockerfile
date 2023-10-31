FROM python:3.8-slim

RUN apt-get update && \
    apt-get install -y default-libmysqlclient-dev pkg-config build-essential

COPY requirements.txt .
COPY bioindex ./bioindex
COPY batch-index-files/index_files.py .
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install mysqlclient


