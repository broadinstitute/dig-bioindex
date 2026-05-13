# NOTE: This image is the legacy build used by external automation. For the
# consolidated bioindex deployment, prefer the top-level Dockerfile, which
# bundles the local source, adds an entrypoint, and sets BIOINDEX_CONFIG_DIR.
FROM python:3.12-slim

WORKDIR /usr/src/app

# install the BioIndex and all its dependencies
RUN pip install git+https://github.com/broadinstitute/dig-bioindex.git@master#egg=bioindex
