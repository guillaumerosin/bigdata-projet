#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
from airflow import DAG
from airflow.operators.python import PythonOperator
from cassandra.auth import PlainTextAuthProvider
from datetime import datetime
from cassandra.cluster import Cluster


# --- CONNEXION AScyllaDB ---
SCYLLA_NODES = ['172.20.0.171', '172.20.0.172', '172.20.0.173']

SCYLLA_USER = 'user_kawasaki'
SCYLLA_PASS = 'wTwF0UQRqL4it4j'
SCYLLA_KEYSPACE = 'keyspace_pour_les_nuls'

# Connexion à ScyllaDB
cluster = Cluster(SCYLLA_NODES, auth_provider=PlainTextAuthProvider(username=SCYLLA_USER, password=SCYLLA_PASS))
session = cluster.connect(SCYLLA_KEYSPACE)

def connexion_établie():
    print(session, "la connexion est établie avec succès ma biche")
    return session

dag = DAG(
    'a1_scylladb_main_parsing',
    description='Connexion à ScyllaDB',
    schedule_interval=None,
    start_date=datetime(year=2026, month=2, day=27),
    catchup=False,

)
task_parsing = PythonOperator(
    task_id='connexion_établie',
    python_callable=connexion_établie,
    dag=dag,
)

