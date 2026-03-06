#!/bin/sh
# Mapper Hadoop Streaming (sans Python) : colonne 4 = source (GDELT GKG), émet source\t1
exec awk -F'\t' 'NF >= 4 && $4 != "" { print $4 "\t1" }'
