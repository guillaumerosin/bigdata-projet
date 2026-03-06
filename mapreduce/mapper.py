#!/usr/bin/env python3
"""
Mapper Hadoop Streaming : extrait la source (colonne 3, format GDELT GKG TSV)
et émet (source, 1) pour chaque ligne.
"""
import sys

COLUMN_SOURCE = 3  # SourceCommonName dans GDELT GKG (0-based)


def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        parts = line.split("\t")
        if COLUMN_SOURCE < len(parts):
            source = parts[COLUMN_SOURCE].strip()
            if source:
                print(f"{source}\t1")


if __name__ == "__main__":
    main()
