#!/bin/bash

index_name=$1
index_path=$2

yes | python3.8 -m bioindex.main index $index_name -l -w 30
