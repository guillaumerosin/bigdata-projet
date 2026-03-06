#!/usr/bin/env python3
"""
Classement des sources de news par nombre d'articles — 3 scripts séparés.

À lancer chacun dans son environnement Docker :

  1. Elasticsearch / Kibana
     python classement_elasticsearch.py [-n 20]

  2. Scylla DB
     python classement_scylla.py [-n 20]

  3. Hadoop / YARN (MapReduce)
     python classement_hadoop.py [-n 20]

Chaque script affiche le top N des sources et une ligne "TEMPS: X.XXX" en fin
de sortie pour comparer les perfs entre les trois runs.
"""
import sys

if __name__ == "__main__":
    print(__doc__)
    print("Fichiers : classement_elasticsearch.py, classement_scylla.py, classement_hadoop.py")
    sys.exit(0)
