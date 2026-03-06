#!/bin/sh
# Reducer Hadoop Streaming (sans Python) : somme les 1 par source, émet source\ttotal
exec awk -F'\t' '{ c[$1] += $2 } END { for (s in c) print s "\t" c[s] }'
