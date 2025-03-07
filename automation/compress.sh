#!/bin/bash

index_name=$1
index_path=$2
env_file="${3:-.bioindex}"

python3.8 -m bioindex.main -e $env_file compress $index_name $index_path
python3.8 -m bioindex.main -e $env_file remove-uncompressed-files $index_name $index_path
python3.8 -m bioindex.main -e $env_file update-compressed-status $index_name $index_path --compressed
