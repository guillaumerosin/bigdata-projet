#!/usr/bin/env python3
"""
Classement des sources de news (sites web) par nombre d'articles — Scylla DB (CQL).

À lancer dans l'environnement Docker Scylla.
Sortie : top N sources + temps en secondes (pour comparer les perfs avec ES et Hadoop).
"""
import argparse
import os
import sys
import time
from collections import Counter

# -----------------------------------------------------------------------------
# CONFIGURATION (adapter ou passer en variables d'environnement)
# -----------------------------------------------------------------------------
SCYLLA_NODES = os.environ.get("SCYLLA_NODES", "172.20.0.171,172.20.0.172,172.20.0.173").split(",")
SCYLLA_USER = os.environ.get("SCYLLA_USER", "user_kawasaki")
SCYLLA_PASS = os.environ.get("SCYLLA_PASS", "wTwF0UQRqL4it4j")
SCYLLA_KEYSPACE = os.environ.get("SCYLLA_KEYSPACE", "gdelt")
SCYLLA_TABLE = os.environ.get("SCYLLA_TABLE", "articles")


def main():
    parser = argparse.ArgumentParser(description="Classement des sources (Scylla CQL)")
    parser.add_argument("-n", "--top", type=int, default=20, help="Nombre de sources à afficher")
    args = parser.parse_args()
    top_n = max(1, args.top)

    try:
        from cassandra.cluster import Cluster
        from cassandra.auth import PlainTextAuthProvider
        from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
    except ImportError:
        print("Erreur: pip install cassandra-driver", file=sys.stderr)
        sys.exit(1)

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
        print(f"Erreur: {e}", file=sys.stderr)
        print(f"TEMPS: {elapsed:.3f}")
        sys.exit(1)
    elapsed = time.perf_counter() - start
    session.shutdown()
    cluster.shutdown()

    print("Scylla DB (CQL) — classement des sources par nombre de news")
    print("=" * 60)
    for i, (source, count) in enumerate(results[:top_n], 1):
        src_short = (source[:50] + "…") if len(source) > 50 else source
        print(f"  {i:3d}. {count:6d}  {src_short}")
    print(f"\nTEMPS: {elapsed:.3f}")


if __name__ == "__main__":
    main()
