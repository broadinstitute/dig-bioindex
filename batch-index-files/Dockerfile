FROM ubuntu:20.04

RUN apt-get update && \
    apt-get install -y git build-essential libbz2-dev liblzma-dev libcurl4-gnutls-dev libssl-dev curl wget zlib1g-dev python3-pip

WORKDIR /usr/src

RUN wget https://github.com/samtools/htslib/releases/download/1.17/htslib-1.17.tar.bz2 && \
    tar -xjf htslib-1.17.tar.bz2 && \
    cd htslib-1.17 && \
    ./configure --enable-libcurl --enable-s3 && \
    make && \
    make install && \
    cd .. && \
    rm -rf htslib-1.17 htslib-1.17.tar.bz2

ENV LD_LIBRARY_PATH=/usr/local/lib

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .


