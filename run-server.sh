#!/usr/bin/env bash
gunicorn -b '127.0.0.1:5000' -k sync -w 1 --threads 4 server:app
