#!/bin/bash

index_name=$1
index_path=$2

python3.8 -m bioindex.main compress $index_name $index_path
python3.8 -m bioindex.main remove-uncompressed-files $index_name $index_path
yes | python3.8 -m bioindex.main index $index_name -l -w 30
