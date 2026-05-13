FROM --platform=linux/amd64 python:3.12-slim AS build

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      default-libmysqlclient-dev pkg-config build-essential curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir mysqlclient

COPY bioindex/ ./bioindex/
COPY web/ ./web/
COPY genes/ ./genes/
COPY setup.py ./
COPY batch-index-files/index_files.py ./

ENV BIOINDEX_CONFIG_DIR=/etc/bioindex
ENV BIOINDEX_WORKERS=2
ENV BIOINDEX_THREAD_POOL=40

RUN groupadd --gid 1000 bioindex && \
    useradd  --uid 1000 --gid bioindex --no-create-home bioindex && \
    chown -R bioindex:bioindex /usr/src/app

USER bioindex

EXPOSE 5000

ENTRYPOINT ["python", "-m", "bioindex.main"]
CMD ["serve", "--port", "5000"]
