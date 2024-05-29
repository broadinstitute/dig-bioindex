#!/bin/bash

python3.8 -m bioindex.main compress dataset-associations associations/dataset/
python3.8 -m bioindex.main remove-uncompressed-files dataset-associations associations/dataset/
yes | python3.8 -m bioindex.main index dataset-associations -l -w 30
