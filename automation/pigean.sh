#!/bin/bash

yes | python3.8 -m bioindex.main index pigean-gene -l -w 30
yes | python3.8 -m bioindex.main index pigean-gene-phenotype -l -w 30
yes | python3.8 -m bioindex.main index pigean-gene-set -l -w 30
yes | python3.8 -m bioindex.main index pigean-gene-set-phenotype -l -w 30
yes | python3.8 -m bioindex.main index pigean-joined-gene -l -w 30
yes | python3.8 -m bioindex.main index pigean-joined-gene-set -l -w 30
