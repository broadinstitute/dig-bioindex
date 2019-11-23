#!/usr/bin/env bash

# NOTE: Don't ever use more than 1 worker as the continuation tokens are only
#       valid on the worker that created them. Instead, use multiple threads.

gunicorn -b '127.0.0.1:5000' -k sync -w 1 --threads 4 server:app
