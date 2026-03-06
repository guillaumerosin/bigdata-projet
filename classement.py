#!/usr/bin/env python3
"""
Classement des sources de news (sites web) par nombre d'articles.

Compare les performances de trois approches sur les mêmes données :
  1. Elasticsearch / Kibana (agrégation)
  2. Scylla DB (CQL + agrégation côté client)
  3. MapReduce Hadoop / YARN (streaming)

Usage:
  python classement.py [--elasticsearch] [--scylla] [--hadoop] [--all]
  Par défaut : --all
"""

import argparse
import os
import subprocess
import sys
import time
from collections import Counter

# -----------------------------------------------------------------------------
# CONFIGURATION (adapter à votre environnement)
# -----------------------------------------------------------------------------

# Elasticsearch (aligné sur dag_kafka_to_elasticsearch.py)
ES_HOST = os.environ.get("ES_HOST", "https://172.20.0.201:9200")
ES_USER = os.environ.get("ES_USER", "user_kawasaki")
ES_PASSWORD = os.environ.get("ES_PASSWORD", "wTwF0UQRqL4it4j")
ES_INDEX = os.environ.get("ES_INDEX", "gdelt-gkg")
ES_SOURCE_FIELD = "source"  # champ contenant le nom du site

# Scylla DB (aligné sur parsing-atest.py)
SCYLLA_NODES = os.environ.get("SCYLLA_NODES", "172.20.0.171,172.20.0.172,172.20.0.173").split(",")
SCYLLA_USER = os.environ.get("SCYLLA_USER", "user_kawasaki")
SCYLLA_PASS = os.environ.get("SCYLLA_PASS", "wTwF0UQRqL4it4j")
SCYLLA_KEYSPACE = "gdelt"
SCYLLA_TABLE = "articles"

# Hadoop (chemin HDFS des données GDELT, format TSV, colonne 3 = source)
HADOOP_INPUT = os.environ.get("HADOOP_INPUT", "/user/gdelt/articles")
HADOOP_OUTPUT = os.environ.get("HADOOP_OUTPUT", "/user/gdelt/classement_sources")
HADOOP_STREAMING_JAR = os.environ.get(
    "HADOOP_STREAMING_JAR",
    "/usr/lib/hadoop-mapreduce/hadoop-streaming.jar"
)
# Répertoire du projet pour mapper/reducer (Hadoop streaming)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def run_elasticsearch_analysis(top_n: int = 50):
    """
    Classement des sources par nombre de news via Elasticsearch (agrégation terms).
    Retourne (liste de (source, count), temps en secondes).
    """
    try:
        from elasticsearch import Elasticsearch
    except ImportError:
        return [], -1.0, "elasticsearch non installé (pip install elasticsearch)"

    es = Elasticsearch(
        [ES_HOST],
        basic_auth=(ES_USER, ES_PASSWORD),
        verify_certs=False,
    )
    start = time.perf_counter()
    try:
        body = {
            "size": 0,
            "aggs": {
                "sources": {
                    "terms": {"field": f"{ES_SOURCE_FIELD}.keyword", "size": top_n, "order": {"_count": "desc"}}
                }
            }
        }
        # Si le champ n'est pas keyword, essayer sans .keyword
        resp = es.search(index=ES_INDEX, body=body)
        agg = resp.get("aggregations", {}).get("sources", {})
        buckets = agg.get("buckets", [])
        if not buckets and ES_SOURCE_FIELD.endswith(".keyword"):
            body["aggs"]["sources"]["terms"]["field"] = ES_SOURCE_FIELD
            resp = es.search(index=ES_INDEX, body=body)
            buckets = resp.get("aggregations", {}).get("sources", {}).get("buckets", [])
        results = [(b["key"], b["doc_count"]) for b in buckets]
    except Exception as e:
        elapsed = time.perf_counter() - start
        return [], elapsed, str(e)
    elapsed = time.perf_counter() - start
    return results, elapsed, None


def run_scylla_analysis(top_n: int = 50):
    """
    Classement des sources par nombre de news via CQL (scan + agrégation en Python).
    Scylla/Cassandra ne fait pas de GROUP BY arbitraire, donc on lit source et on compte.
    Retourne (liste de (source, count), temps en secondes).
    """
    try:
        from cassandra.cluster import Cluster
        from cassandra.auth import PlainTextAuthProvider
        from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
    except ImportError:
        return [], -1.0, "cassandra-driver non installé (pip install cassandra-driver)"

    auth = PlainTextAuthProvider(username=SCYLLA_USER, password=SCYLLA_PASS)
    cluster = Cluster(
        contact_points=SCYLLA_NODES,
        port=9042,
        auth_provider=auth,
        protocol_version=4,
        load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy(local_dc="datacenter1")),
    )
    session = cluster.connect(SCYLLA_KEYSPACE)
    start = time.perf_counter()
    try:
        rows = session.execute(f"SELECT source FROM {SCYLLA_TABLE}")
        counter = Counter()
        for row in rows:
            src = (row.source or "").strip()
            if src:
                counter[src] += 1
        results = counter.most_common(top_n)
    except Exception as e:
        elapsed = time.perf_counter() - start
        session.shutdown()
        cluster.shutdown()
        return [], elapsed, str(e)
    elapsed = time.perf_counter() - start
    session.shutdown()
    cluster.shutdown()
    return results, elapsed, None


def run_hadoop_mapreduce_analysis(top_n: int = 50):
    """
    Classement des sources via MapReduce Hadoop (streaming).
    Utilise mapreduce/mapper.py et mapreduce/reducer.py.
    Retourne (liste de (source, count), temps en secondes).
    """
    mapper = os.path.join(SCRIPT_DIR, "mapreduce", "mapper.py")
    reducer = os.path.join(SCRIPT_DIR, "mapreduce", "reducer.py")
    if not os.path.isfile(mapper) or not os.path.isfile(reducer):
        return [], -1.0, f"Fichiers mapreduce manquants: {mapper} / {reducer}"

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
            return [], elapsed, proc.stderr or proc.stdout or "Erreur Hadoop"
        # Lire le résultat HDFS: part-*
        cat_proc = subprocess.run(
            ["hadoop", "fs", "-cat", f"{out}/part-*"],
            capture_output=True, text=True, timeout=60
        )
        if cat_proc.returncode != 0:
            return [], elapsed, "Impossible de lire la sortie HDFS"
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
        return results, elapsed, None
    except subprocess.TimeoutExpired:
        elapsed = time.perf_counter() - start
        return [], elapsed, "Timeout Hadoop"
    except FileNotFoundError:
        return [], -1.0, "hadoop non trouvé (Hadoop pas installé ou pas dans PATH)"
    except Exception as e:
        return [], time.perf_counter() - start, str(e)


def print_results(name: str, results: list, elapsed: float, error: str | None, top_n: int):
    print(f"\n{'='*60}")
    print(f"  {name}")
    print("=" * 60)
    if error:
        print(f"  Erreur: {error}")
        if elapsed >= 0:
            print(f"  Temps: {elapsed:.3f} s")
        return
    print(f"  Temps: {elapsed:.3f} s")
    print(f"  Top {min(top_n, len(results))} sources:")
    for i, (source, count) in enumerate(results[:top_n], 1):
        src_short = (source[:50] + "…") if len(source) > 50 else source
        print(f"    {i:3d}. {count:6d}  {src_short}")
    print()


def main():
    parser = argparse.ArgumentParser(description="Classement des sources par nombre de news (ES / Scylla / Hadoop)")
    parser.add_argument("--elasticsearch", action="store_true", help="Lancer uniquement l’analyse Elasticsearch")
    parser.add_argument("--scylla", action="store_true", help="Lancer uniquement l’analyse Scylla CQL")
    parser.add_argument("--hadoop", action="store_true", help="Lancer uniquement l’analyse MapReduce Hadoop")
    parser.add_argument("--all", action="store_true", default=True, help="Lancer les trois et comparer (défaut)")
    parser.add_argument("-n", "--top", type=int, default=20, help="Nombre de sources à afficher (défaut: 20)")
    args = parser.parse_args()

    if not (args.elasticsearch or args.scylla or args.hadoop):
        args.elasticsearch = args.scylla = args.hadoop = True

    top_n = max(1, args.top)
    timings = {}

    if args.elasticsearch:
        res, elapsed, err = run_elasticsearch_analysis(top_n=top_n)
        timings["Elasticsearch"] = elapsed if err is None else None
        print_results("Elasticsearch / Kibana (agrégation)", res, elapsed, err, top_n)

    if args.scylla:
        res, elapsed, err = run_scylla_analysis(top_n=top_n)
        timings["Scylla DB (CQL)"] = elapsed if err is None else None
        print_results("Scylla DB (CQL + agrégation client)", res, elapsed, err, top_n)

    if args.hadoop:
        res, elapsed, err = run_hadoop_mapreduce_analysis(top_n=top_n)
        timings["Hadoop MapReduce"] = elapsed if err is None else None
        print_results("Hadoop / YARN (MapReduce streaming)", res, elapsed, err, top_n)

    # Comparaison des perfs
    valid = [(k, v) for k, v in timings.items() if v is not None and v >= 0]
    if len(valid) >= 2:
        print("=" * 60)
        print("  COMPARAISON DES PERFORMANCES")
        print("=" * 60)
        for name, t in sorted(valid, key=lambda x: x[1]):
            print(f"  {name}: {t:.3f} s")
        print()


if __name__ == "__main__":
    main()
