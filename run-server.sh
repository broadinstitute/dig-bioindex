#!/usr/bin/env bash

# NOTE: Always use 1 worker per thread so that continuations work!
gunicorn -b '127.0.0.1:5000' -k sync -w 1 --threads 4 server:app
