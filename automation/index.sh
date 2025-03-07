#!/bin/bash

index_name=$1
index_path=$2
env_file="${3:-.bioindex}"

yes | python3.8 -m bioindex.main -e $env_file index $index_name -l -w 30
