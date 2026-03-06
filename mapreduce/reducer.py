#!/usr/bin/env python3
"""
Reducer Hadoop Streaming : somme les émissions (source, 1) du mapper
et émet (source, total).
"""
import sys

def main():
    current_source = None
    total = 0
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        parts = line.split("\t", 1)
        if len(parts) != 2:
            continue
        source, one = parts
        try:
            one = int(one)
        except ValueError:
            continue
        if source == current_source:
            total += one
        else:
            if current_source is not None:
                print(f"{current_source}\t{total}")
            current_source = source
            total = one
    if current_source is not None:
        print(f"{current_source}\t{total}")


if __name__ == "__main__":
    main()
