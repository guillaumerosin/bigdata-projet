#!/usr/bin/env python3
"""
Classement des sources de news (sites web) par nombre d'articles — Elasticsearch.

À lancer dans l'environnement Docker Elasticsearch / Kibana.
Sortie : top N sources + temps en secondes (pour comparer les perfs avec Scylla et Hadoop).
"""
import argparse
import os
import sys
import time

# -----------------------------------------------------------------------------
# CONFIGURATION (adapter ou passer en variables d'environnement)
# -----------------------------------------------------------------------------
ES_HOST = os.environ.get("ES_HOST", "https://172.20.0.201:9200")
ES_USER = os.environ.get("ES_USER", "user_kawasaki")
ES_PASSWORD = os.environ.get("ES_PASSWORD", "wTwF0UQRqL4it4j")
ES_INDEX = os.environ.get("ES_INDEX", "gdelt-gkg")
ES_SOURCE_FIELD = os.environ.get("ES_SOURCE_FIELD", "source")


def main():
    parser = argparse.ArgumentParser(description="Classement des sources (Elasticsearch)")
    parser.add_argument("-n", "--top", type=int, default=20, help="Nombre de sources à afficher")
    args = parser.parse_args()
    top_n = max(1, args.top)

    try:
        from elasticsearch import Elasticsearch
    except ImportError:
        print("Erreur: pip install elasticsearch", file=sys.stderr)
        sys.exit(1)

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
        resp = es.search(index=ES_INDEX, body=body)
        agg = resp.get("aggregations", {}).get("sources", {})
        buckets = agg.get("buckets", [])
        if not buckets:
            body["aggs"]["sources"]["terms"]["field"] = ES_SOURCE_FIELD
            resp = es.search(index=ES_INDEX, body=body)
            buckets = resp.get("aggregations", {}).get("sources", {}).get("buckets", [])
        results = [(b["key"], b["doc_count"]) for b in buckets]
    except Exception as e:
        elapsed = time.perf_counter() - start
        print(f"Erreur: {e}", file=sys.stderr)
        print(f"TEMPS: {elapsed:.3f}")
        sys.exit(1)

    elapsed = time.perf_counter() - start
    print("Elasticsearch — classement des sources par nombre de news")
    print("=" * 60)
    for i, (source, count) in enumerate(results[:top_n], 1):
        src_short = (source[:50] + "…") if len(source) > 50 else source
        print(f"  {i:3d}. {count:6d}  {src_short}")
    print(f"\nTEMPS: {elapsed:.3f}")


if __name__ == "__main__":
    main()
