#!/usr/bin/env python3
"""
Classement des sources de news (sites web) par nombre d'articles — MapReduce Hadoop / YARN.

À lancer dans l'environnement Docker Hadoop (avec hadoop-streaming.jar).
Utilise mapreduce/mapper.py et mapreduce/reducer.py.
Sortie : top N sources + temps en secondes (pour comparer les perfs avec ES et Scylla).
"""
import argparse
import os
import subprocess
import sys
import time

# -----------------------------------------------------------------------------
# CONFIGURATION (adapter ou passer en variables d'environnement)
# -----------------------------------------------------------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
HADOOP_INPUT = os.environ.get("HADOOP_INPUT", "/user/gdelt/articles")
HADOOP_OUTPUT = os.environ.get("HADOOP_OUTPUT", "/user/gdelt/classement_sources")
HADOOP_STREAMING_JAR = os.environ.get(
    "HADOOP_STREAMING_JAR",
    "/usr/lib/hadoop-mapreduce/hadoop-streaming.jar",
)


def main():
    parser = argparse.ArgumentParser(description="Classement des sources (Hadoop MapReduce)")
    parser.add_argument("-n", "--top", type=int, default=20, help="Nombre de sources à afficher")
    args = parser.parse_args()
    top_n = max(1, args.top)

    mapper = os.path.join(SCRIPT_DIR, "mapreduce", "mapper.py")
    reducer = os.path.join(SCRIPT_DIR, "mapreduce", "reducer.py")
    if not os.path.isfile(mapper) or not os.path.isfile(reducer):
        print(f"Erreur: mapper ou reducer manquant ({mapper}, {reducer})", file=sys.stderr)
        sys.exit(1)

    out = f"{HADOOP_OUTPUT}_{int(time.time())}"
    cmd = [
        "hadoop", "jar", HADOOP_STREAMING_JAR,
        "-input", HADOOP_INPUT,
        "-output", out,
        "-mapper", "python3 mapper.py",
        "-reducer", "python3 reducer.py",
        "-file", mapper,
        "-file", reducer,
    ]
    start = time.perf_counter()
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
        elapsed = time.perf_counter() - start
        if proc.returncode != 0:
            print(proc.stderr or proc.stdout or "Erreur Hadoop", file=sys.stderr)
            print(f"TEMPS: {elapsed:.3f}")
            sys.exit(1)
        cat_proc = subprocess.run(
            ["hadoop", "fs", "-cat", f"{out}/part-*"],
            capture_output=True, text=True, timeout=60,
        )
        if cat_proc.returncode != 0:
            print("Impossible de lire la sortie HDFS", file=sys.stderr)
            print(f"TEMPS: {elapsed:.3f}")
            sys.exit(1)
        results = []
        for line in cat_proc.stdout.strip().splitlines():
            parts = line.split("\t", 1)
            if len(parts) == 2:
                try:
                    results.append((parts[0], int(parts[1])))
                except ValueError:
                    pass
        results.sort(key=lambda x: x[1], reverse=True)
        results = results[:top_n]
    except subprocess.TimeoutExpired:
        elapsed = time.perf_counter() - start
        print("Timeout Hadoop", file=sys.stderr)
        print(f"TEMPS: {elapsed:.3f}")
        sys.exit(1)
    except FileNotFoundError:
        print("hadoop non trouvé (PATH)", file=sys.stderr)
        print("TEMPS: -1.000")
        sys.exit(1)
    except Exception as e:
        elapsed = time.perf_counter() - start
        print(f"Erreur: {e}", file=sys.stderr)
        print(f"TEMPS: {elapsed:.3f}")
        sys.exit(1)

    print("Hadoop MapReduce — classement des sources par nombre de news")
    print("=" * 60)
    for i, (source, count) in enumerate(results[:top_n], 1):
        src_short = (source[:50] + "…") if len(source) > 50 else source
        print(f"  {i:3d}. {count:6d}  {src_short}")
    print(f"\nTEMPS: {elapsed:.3f}")


if __name__ == "__main__":
    main()
