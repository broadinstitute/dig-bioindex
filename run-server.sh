#!/usr/bin/env bash

uvicorn server:app --host 0.0.0.0 --port 5000 --env-file .bioindex
