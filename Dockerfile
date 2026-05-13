FROM --platform=linux/amd64 python:3.12-slim AS build

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      default-libmysqlclient-dev pkg-config build-essential curl \
      libcurl4-openssl-dev libssl-dev libbz2-dev liblzma-dev zlib1g-dev \
      wget ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# htslib provides bgzip; built from source with S3 + libcurl support so
# the bioindex reader can read compressed indexes directly from S3.
ARG HTSLIB_VERSION=1.21
RUN cd /tmp && \
    wget -q https://github.com/samtools/htslib/releases/download/${HTSLIB_VERSION}/htslib-${HTSLIB_VERSION}.tar.bz2 && \
    tar -xjf htslib-${HTSLIB_VERSION}.tar.bz2 && \
    cd htslib-${HTSLIB_VERSION} && \
    ./configure --enable-s3 --enable-libcurl && \
    make -j$(nproc) && \
    make install && \
    ldconfig && \
    cd / && rm -rf /tmp/htslib-*

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

# Default continuation-token signing key. Bioindex serves only public data,
# so a stable key in the image is acceptable: forging a token at most yields
# garbled iteration over already-public data, not an authorization bypass.
# Override at runtime (-e BIOINDEX_TOKEN_SIGNING_KEY=...) if you ever want
# unique keys per environment.
ENV BIOINDEX_TOKEN_SIGNING_KEY=0000000000000000000000000000000000000000000000000000000000000000

RUN groupadd --gid 1000 bioindex && \
    useradd  --uid 1000 --gid bioindex --no-create-home bioindex && \
    chown -R bioindex:bioindex /usr/src/app

USER bioindex

EXPOSE 5000

ENTRYPOINT ["python", "-m", "bioindex.main"]
CMD ["serve", "--port", "5000"]
